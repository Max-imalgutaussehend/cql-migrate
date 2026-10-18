use cql_migrate::migration::{analyse, load_dir, next_file, parse_file_name};
use cql_migrate::model::{Change, diff, render, replay, schema_of_files};

fn write(dir: &std::path::Path, name: &str, text: &str) {
    std::fs::write(dir.join(name), text).unwrap();
}

#[test]
fn file_names() {
    assert_eq!(
        parse_file_name("0001_create_users.cql"),
        Some((1, "create_users".into()))
    );
    assert_eq!(parse_file_name("12_x.cql"), Some((12, "x".into())));
    assert_eq!(parse_file_name("create_users.cql"), None);
    assert_eq!(parse_file_name("0001.cql"), None);
    assert_eq!(parse_file_name("0001_x.sql"), None);
}

#[test]
fn checksum_ignores_comments_whitespace_and_keyword_case() {
    let a = analyse("CREATE TABLE t (a int PRIMARY KEY);").unwrap().1;
    let b = analyse("-- hello\ncreate   table t (\n  a int primary key\n) ;\n")
        .unwrap()
        .1;
    assert_eq!(a, b);
    let c = analyse("CREATE TABLE t (a bigint PRIMARY KEY);").unwrap().1;
    assert_ne!(a, c);
    let d = analyse("INSERT INTO t (a) VALUES ('x');").unwrap().1;
    let e = analyse("INSERT INTO t (a) VALUES ('X');").unwrap().1;
    assert_ne!(d, e, "string literals are significant");
}

#[test]
fn statements_and_batches() {
    let (s, _) = analyse("CREATE TABLE a (x int PRIMARY KEY);\nBEGIN BATCH INSERT INTO a (x) VALUES (1); INSERT INTO a (x) VALUES (2); APPLY BATCH;\nDROP TABLE a").unwrap();
    assert_eq!(s.len(), 3);
    assert!(s[1].starts_with("BEGIN BATCH") && s[1].ends_with("APPLY BATCH"));
    assert_eq!(s[2], "DROP TABLE a");
}

#[test]
fn directory_loading_and_errors() {
    let d = tempfile::tempdir().unwrap();
    write(
        d.path(),
        "0002_b.cql",
        "CREATE TABLE b (x int PRIMARY KEY);",
    );
    write(
        d.path(),
        "0001_a.cql",
        "CREATE TABLE a (x int PRIMARY KEY);",
    );
    write(d.path(), "README.md", "ignored");
    let ms = load_dir(d.path()).unwrap();
    assert_eq!(ms.iter().map(|m| m.version).collect::<Vec<_>>(), [1, 2]);
    write(d.path(), "0001_dup.cql", "SELECT 1 FROM t;");
    assert!(
        load_dir(d.path())
            .unwrap_err()
            .to_string()
            .contains("used twice")
    );
    std::fs::remove_file(d.path().join("0001_dup.cql")).unwrap();
    write(d.path(), "badname.cql", "");
    assert!(load_dir(d.path()).is_err());
}

#[test]
fn next_file_numbering() {
    let d = tempfile::tempdir().unwrap();
    assert_eq!(
        next_file(d.path(), "Create Users!")
            .unwrap()
            .file_name()
            .unwrap(),
        "0001_create_users.cql"
    );
    write(d.path(), "0007_x.cql", "");
    assert_eq!(
        next_file(d.path(), "add email")
            .unwrap()
            .file_name()
            .unwrap(),
        "0008_add_email.cql"
    );
    assert!(next_file(d.path(), "  ").is_err());
}

#[test]
fn chain_replay_finds_errors() {
    let d = tempfile::tempdir().unwrap();
    write(
        d.path(),
        "0001_a.cql",
        "CREATE TABLE ks.a (id int PRIMARY KEY, v text);",
    );
    write(d.path(), "0002_b.cql", "ALTER TABLE ks.a ADD w int;");
    let ok = replay(&load_dir(d.path()).unwrap());
    assert!(ok.problems.is_empty(), "{:?}", ok.problems);
    assert_eq!(ok.schema.tables[0].columns.len(), 3);
    write(d.path(), "0003_c.cql", "ALTER TABLE ks.nope ADD w int;");
    let bad = replay(&load_dir(d.path()).unwrap());
    assert_eq!(bad.problems.len(), 1);
    assert!(bad.problems[0].0.contains("0003_c"));
}

#[test]
fn data_statements_are_not_part_of_the_schema() {
    let d = tempfile::tempdir().unwrap();
    write(
        d.path(),
        "0001_a.cql",
        "CREATE TABLE ks.a (id int PRIMARY KEY);\nINSERT INTO ks.a (id) VALUES (1);",
    );
    assert!(replay(&load_dir(d.path()).unwrap()).problems.is_empty());
}

#[test]
fn diff_generates_create_and_add_column() {
    let have = replay(&{
        let d = tempfile::tempdir().unwrap();
        write(
            d.path(),
            "0001_a.cql",
            "CREATE TABLE ks.a (id int PRIMARY KEY, v text);",
        );
        load_dir(d.path()).unwrap()
    });
    let desired = "CREATE TABLE ks.a (id int PRIMARY KEY, v text, w list<int>);\nCREATE TABLE ks.b (x int, y text, PRIMARY KEY (x)) WITH comment = 'new';";
    let files = vec![("schema.cql".to_string(), desired.to_string())];
    let want = schema_of_files(&files);
    let changes = diff(&have.schema, &want.schema);
    assert!(changes.contains(&Change::AddColumn {
        table: "ks.a".into(),
        column: "w".into(),
        ty: "list<int>".into()
    }));
    assert!(changes.contains(&Change::CreateTable {
        table: "ks.b".into()
    }));
    let sql = render(&changes, &want.schema, &files, false);
    assert!(sql.contains("ALTER TABLE ks.a ADD w list<int>;"), "{sql}");
    assert!(
        sql.contains("CREATE TABLE ks.b (x int, y text, PRIMARY KEY (x)) WITH comment = 'new';"),
        "{sql}"
    );
}

#[test]
fn diff_flags_destructive_and_impossible_changes() {
    let have = replay(&{
        let d = tempfile::tempdir().unwrap();
        write(
            d.path(),
            "0001_a.cql",
            "CREATE TABLE ks.a (id int, c int, v text, PRIMARY KEY (id, c));\nCREATE TABLE ks.gone (id int PRIMARY KEY);",
        );
        load_dir(d.path()).unwrap()
    });
    let files = vec![(
        "s.cql".to_string(),
        "CREATE TABLE ks.a (id int, c int, v int, PRIMARY KEY (id));".to_string(),
    )];
    let want = schema_of_files(&files);
    let changes = diff(&have.schema, &want.schema);
    assert!(
        changes
            .iter()
            .any(|c| matches!(c, Change::TypeChanged { column, .. } if column == "v"))
    );
    assert!(
        changes
            .iter()
            .any(|c| matches!(c, Change::KeyChanged { .. }))
    );
    assert!(changes.contains(&Change::DropTable {
        table: "ks.gone".into()
    }));
    let safe = render(&changes, &want.schema, &files, false);
    assert!(safe.contains("-- destructive, review: DROP TABLE ks.gone;"));
    assert!(render(&changes, &want.schema, &files, true).contains("\nDROP TABLE ks.gone;"));
}

#[test]
fn no_diff_when_equal() {
    let files = vec![(
        "s.cql".to_string(),
        "CREATE TABLE ks.a (id int PRIMARY KEY);".to_string(),
    )];
    let want = schema_of_files(&files);
    assert!(diff(&want.schema, &want.schema).is_empty());
}
