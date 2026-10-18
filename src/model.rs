//! The schema a migration chain produces, checked and diffed offline with `cqlc-core`.

use crate::migration::{Migration, analyse};
use cqlc_core::schema::{Schema, Table};
use cqlc_core::{Input, compile};
use std::collections::BTreeMap;

pub struct Model {
    pub schema: Schema,
    /// Problems found while replaying the chain: (migration, message).
    pub problems: Vec<(String, String)>,
}

fn is_ddl(stmt: &str) -> bool {
    matches!(
        stmt.split_whitespace()
            .next()
            .map(|w| w.to_ascii_uppercase())
            .as_deref(),
        Some("CREATE" | "ALTER" | "DROP")
    )
}

/// Replays the DDL of all migrations in order.
pub fn replay(migrations: &[Migration]) -> Model {
    let mut inputs = Vec::new();
    for m in migrations {
        let text: String = m
            .statements
            .iter()
            .filter(|s| is_ddl(s))
            .map(|s| format!("{s};\n"))
            .collect();
        inputs.push((m, Input::new(m.path.display().to_string(), text)));
    }
    let c = compile(
        &inputs.iter().map(|(_, i)| i.clone()).collect::<Vec<_>>(),
        &[],
    );
    let mut problems = Vec::new();
    for d in c.diagnostics.iter().filter(|d| d.is_error()) {
        let file = c.sources.get(d.span.file).name.clone();
        problems.push((file, d.message.clone()));
    }
    Model {
        schema: c.schema,
        problems,
    }
}

/// Replays plain schema files (the desired state).
pub fn schema_of_files(files: &[(String, String)]) -> Model {
    let inputs: Vec<Input> = files
        .iter()
        .map(|(n, t)| {
            let stmts = analyse(t).map(|(s, _)| s).unwrap_or_default();
            Input::new(
                n,
                stmts
                    .iter()
                    .filter(|s| is_ddl(s))
                    .map(|s| format!("{s};\n"))
                    .collect::<String>(),
            )
        })
        .collect();
    let c = compile(&inputs, &[]);
    let problems = c
        .diagnostics
        .iter()
        .filter(|d| d.is_error())
        .map(|d| (c.sources.get(d.span.file).name.clone(), d.message.clone()))
        .collect();
    Model {
        schema: c.schema,
        problems,
    }
}

fn qname(t: &Table) -> String {
    t.qualified_name()
}

fn col_map(t: &Table) -> BTreeMap<&str, String> {
    t.columns
        .iter()
        .map(|c| {
            (
                c.name.as_str(),
                format!("{}{}", c.cql_type, if c.is_static { " static" } else { "" }),
            )
        })
        .collect()
}

fn key_of(t: &Table) -> String {
    format!(
        "({}), {}",
        t.partition_key.join(", "),
        t.clustering
            .iter()
            .map(|c| format!("{} {:?}", c.name, c.order))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

#[derive(Clone, Debug, PartialEq)]
pub enum Change {
    CreateTable {
        table: String,
    },
    DropTable {
        table: String,
    },
    AddColumn {
        table: String,
        column: String,
        ty: String,
    },
    DropColumn {
        table: String,
        column: String,
    },
    /// Cassandra cannot change a column's type in place (except a few compatible widenings it has dropped).
    TypeChanged {
        table: String,
        column: String,
        from: String,
        to: String,
    },
    /// Primary keys are immutable.
    KeyChanged {
        table: String,
        from: String,
        to: String,
    },
}

/// Differences that take `have` (what the migrations produce) to `want` (the desired schema).
pub fn diff(have: &Schema, want: &Schema) -> Vec<Change> {
    let mut out = Vec::new();
    let h: BTreeMap<String, &Table> = have.tables.iter().map(|t| (qname(t), t)).collect();
    let w: BTreeMap<String, &Table> = want.tables.iter().map(|t| (qname(t), t)).collect();
    for (n, wt) in &w {
        match h.get(n) {
            None => out.push(Change::CreateTable { table: n.clone() }),
            Some(ht) => {
                if key_of(ht) != key_of(wt) {
                    out.push(Change::KeyChanged {
                        table: n.clone(),
                        from: key_of(ht),
                        to: key_of(wt),
                    });
                }
                let (hc, wc) = (col_map(ht), col_map(wt));
                for (c, ty) in &wc {
                    match hc.get(c) {
                        None => out.push(Change::AddColumn {
                            table: n.clone(),
                            column: (*c).into(),
                            ty: ty.clone(),
                        }),
                        Some(old) if old != ty => out.push(Change::TypeChanged {
                            table: n.clone(),
                            column: (*c).into(),
                            from: old.clone(),
                            to: ty.clone(),
                        }),
                        _ => {}
                    }
                }
                for c in hc.keys() {
                    if !wc.contains_key(c) {
                        out.push(Change::DropColumn {
                            table: n.clone(),
                            column: (*c).into(),
                        });
                    }
                }
            }
        }
    }
    for n in h.keys() {
        if !w.contains_key(n) {
            out.push(Change::DropTable { table: n.clone() });
        }
    }
    out
}

/// The text of the statement that defines `table` in `source` (for `CREATE TABLE` of new tables).
pub fn create_statement(source: &str, table: &Table) -> Option<String> {
    let (stmts, _) = analyse(source).ok()?;
    let want = table.name.to_ascii_lowercase();
    stmts.into_iter().find(|s| {
        let up = s.to_ascii_uppercase();
        (up.starts_with("CREATE TABLE") || up.starts_with("CREATE COLUMNFAMILY"))
            && s.to_ascii_lowercase()
                .split(|c: char| !(c.is_alphanumeric() || c == '_' || c == '.'))
                .any(|w| w.rsplit('.').next() == Some(want.as_str()))
    })
}

/// Renders the CQL for `changes`. Destructive changes are emitted as comments unless `allow_destructive`.
pub fn render(
    changes: &[Change],
    want: &Schema,
    sources: &[(String, String)],
    allow_destructive: bool,
) -> String {
    let mut out = String::new();
    for c in changes {
        match c {
            Change::CreateTable { table } => {
                let t = want.tables.iter().find(|t| &qname(t) == table).unwrap();
                let stmt = sources.iter().find_map(|(_, src)| create_statement(src, t));
                match stmt {
                    Some(s) => out.push_str(&format!("{};\n\n", s)),
                    None => out.push_str(&format!("-- TODO: could not find the CREATE statement of {table}\n\n")),
                }
            }
            Change::AddColumn { table, column, ty } => out.push_str(&format!("ALTER TABLE {table} ADD {column} {ty};\n")),
            Change::DropColumn { table, column } => {
                let s = format!("ALTER TABLE {table} DROP {column};");
                out.push_str(&if allow_destructive { format!("{s}\n") } else { format!("-- destructive, review: {s}\n") });
            }
            Change::DropTable { table } => {
                let s = format!("DROP TABLE {table};");
                out.push_str(&if allow_destructive { format!("{s}\n") } else { format!("-- destructive, review: {s}\n") });
            }
            Change::TypeChanged { table, column, from, to } => out.push_str(&format!("-- cannot be done in place: {table}.{column} changes from {from} to {to}; add a new column and migrate the data\n")),
            Change::KeyChanged { table, from, to } => out.push_str(&format!("-- cannot be done in place: primary key of {table} changes from {from} to {to}; create a new table and migrate the data\n")),
        }
    }
    out
}

/// Normalises a type string from `system_schema.columns` for comparison with the model.
pub fn norm_type(t: &str) -> String {
    t.to_ascii_lowercase()
        .replace("varchar", "text")
        .replace(' ', "")
}

/// (table -> column -> type) of the model, normalised for drift comparison.
pub fn columns_of(schema: &Schema) -> BTreeMap<String, BTreeMap<String, String>> {
    schema
        .tables
        .iter()
        .map(|t| {
            (
                qname(t).to_ascii_lowercase(),
                t.columns
                    .iter()
                    .map(|c| {
                        (
                            c.name.to_ascii_lowercase(),
                            norm_type(&c.cql_type.to_string()),
                        )
                    })
                    .collect(),
            )
        })
        .collect()
}
