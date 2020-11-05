/*
 * Copyright 2020 sukawasatoru
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::sync::Arc;

pub fn column<T, A>(name: T, column_type: Type, attributes: A) -> Arc<Column>
where
    T: Into<String>,
    A: Into<Vec<Attribute>>,
{
    let attributes = match attributes.into() {
        data if data.is_empty() => None,
        data => Some(data),
    };
    Arc::new(Column::Column {
        name: name.into(),
        column_type,
        attributes,
    })
}

pub fn primary_key<K: AsRef<[Arc<Column>]>>(keys: K) -> Arc<Column> {
    Arc::new(Column::Constraint(format!(
        "{} ({})",
        Attribute::PRIMARY_KEY.name(),
        keys.as_ref()
            .iter()
            .map(|data| data.name().to_owned())
            .collect::<Vec<_>>()
            .join(", ")
    )))
}

pub fn foreign_key<T: Into<TableName>, A: Into<Vec<ForeignKeyAttribute>>>(
    column_name: Arc<Column>,
    references: ForeignKeyAttribute,
    other_table_name: T,
    other_table_column: Arc<Column>,
    attributes: A,
) -> Arc<Column> {
    Arc::new(Column::Constraint(format!(
        "FOREIGN KEY ({column_name}) {references} {other_table_name} ({other_table_column}){attributes}",
        column_name = column_name.name(),
        references = references,
        other_table_name = other_table_name.into().0,
        other_table_column = other_table_column.name(),
        attributes = {
            let attributes = attributes.into();
            if attributes.is_empty() {
                "".to_string()
            } else {
                format!(" {}", attributes.iter().map(ToString::to_string).collect::<Vec<_>>().join(" "))
            }
        }
    )))
}

pub fn unique<K: AsRef<[Arc<Column>]>>(keys: K) -> Arc<Column> {
    Arc::new(Column::Constraint(format!(
        "{} ({})",
        Attribute::UNIQUE.name(),
        keys.as_ref()
            .iter()
            .map(|data| data.name())
            .collect::<Vec<_>>()
            .join(", ")
    )))
}

#[allow(non_camel_case_types)]
pub enum Type {
    INTEGER,
    INT,
    TINYINT,
    SMALLINT,
    MEDIUMINT,
    BIGINT,
    UNSIGNED_BIG_INT,
    INT2,
    INT8,
    TEXT,
    CLOB,
    BLOB,
    REAL,
    DOUBLE,
    DOUBLE_PRECISION,
    FLOAT,
    NUMERIC,
    BOOLEAN,
    DATE,
    DATETIME,
}

impl Type {
    fn name(&self) -> &str {
        match self {
            Type::INTEGER => "INTEGER",
            Type::INT => "INT",
            Type::TINYINT => "TINYINT",
            Type::SMALLINT => "SMALLINT",
            Type::MEDIUMINT => "MEDIUMINT",
            Type::BIGINT => "BIGINT",
            Type::UNSIGNED_BIG_INT => "UNSIGNED BIG INT",
            Type::INT2 => "INT2",
            Type::INT8 => "INT8",
            Type::TEXT => "TEXT",
            Type::CLOB => "CLOB",
            Type::BLOB => "BLOB",
            Type::REAL => "REAL",
            Type::DOUBLE => "DOUBLE",
            Type::DOUBLE_PRECISION => "DOUBLE PRECISION",
            Type::FLOAT => "FLOAT",
            Type::NUMERIC => "NUMERIC",
            Type::BOOLEAN => "BOOLEAN",
            Type::DATE => "DATE",
            Type::DATETIME => "DATETIME",
        }
    }
}

#[allow(non_camel_case_types)]
pub enum Attribute {
    PRIMARY_KEY,
    ASC,
    DESC,
    UNIQUE,
    NOT_NULL,
    AUTOINCREMENT,
    DEFAULT(String),
}

impl Attribute {
    fn name(&self) -> String {
        match self {
            Attribute::PRIMARY_KEY => "PRIMARY KEY".to_owned(),
            Attribute::ASC => "ASC".to_owned(),
            Attribute::DESC => "DESC".to_owned(),
            Attribute::UNIQUE => "UNIQUE".to_owned(),
            Attribute::NOT_NULL => "NOT NULL".to_owned(),
            Attribute::AUTOINCREMENT => "AUTOINCREMENT".to_owned(),
            Attribute::DEFAULT(value) => format!("DEFAULT {}", escape_string(value)),
        }
    }
}

#[allow(non_camel_case_types)]
pub enum ForeignKeyAttribute {
    REFERENCES,
    ON_DELETE,
    ON_UPDATE,
    SET_NULL,
    SET_DEFAULT,
    CASCADE,
    RESTRICT,
    NO_ACTION,
    DEFERRABLE_INITIALLY_DEFERRED,
}

impl std::fmt::Display for ForeignKeyAttribute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use ForeignKeyAttribute::*;
        match self {
            REFERENCES => write!(f, "REFERENCES"),
            ON_DELETE => write!(f, "ON DELETE"),
            ON_UPDATE => write!(f, "ON UPDATE"),
            SET_NULL => write!(f, "SET NULL"),
            SET_DEFAULT => write!(f, "SET DEFAULT"),
            CASCADE => write!(f, "CASCADE"),
            RESTRICT => write!(f, "RESTRICT"),
            NO_ACTION => write!(f, "NO ACTION"),
            DEFERRABLE_INITIALLY_DEFERRED => write!(f, "DEFERRABLE INITIALLY DEFERRED"),
        }
    }
}

fn escape_string<T: Into<String>>(value: T) -> String {
    let value = value.into();
    if value.contains('\'') {
        format!("\'{}\'", value.replace("\'", "\'\'"))
    } else {
        format!("\'{}\'", value)
    }
}

pub trait Table {
    fn name(&self) -> &str;

    fn columns(&self) -> &[Arc<Column>];

    fn create_sql(&self) -> String {
        format!(
            "CREATE TABLE {} ({})",
            self.name(),
            self.columns()
                .iter()
                .map(|data| data.create_statement())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

pub enum Column {
    Column {
        name: String,
        column_type: Type,
        attributes: Option<Vec<Attribute>>,
    },
    Constraint(String),
}

impl Column {
    pub fn name(&self) -> &str {
        match self {
            Column::Column { name, .. } => name,
            _ => panic!(),
        }
    }

    fn create_statement(&self) -> String {
        match self {
            Column::Column {
                name,
                column_type,
                attributes,
            } => match attributes {
                Some(attributes) => format!(
                    "{} {} {}",
                    name,
                    column_type.name(),
                    attributes
                        .iter()
                        .map(|data| data.name())
                        .collect::<Vec<_>>()
                        .join(" ")
                ),
                None => format!("{} {}", name, column_type.name()),
            },
            Column::Constraint(value) => value.into(),
        }
    }

    pub fn create_add_sql(&self) -> String {
        match self {
            Column::Column { name, .. } => {
                format!("ALTER TABLE {} ADD {}", name, self.create_statement())
            }
            _ => panic!(),
        }
    }
}

pub struct TableName(String);

impl<T: Table> From<&T> for TableName {
    fn from(value: &T) -> Self {
        Self(value.name().into())
    }
}

impl From<String> for TableName {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for TableName {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}

#[cfg(test)]
mod tests {
    use crate::Attribute::*;
    use crate::ForeignKeyAttribute::*;
    use crate::Type::*;
    use crate::*;
    use rusqlite::params;
    use std::sync::Arc;

    #[test]
    fn empty_arr() {
        assert_eq!(
            (&[][..] as &[&str])
                .iter()
                .map(|data| data.to_owned())
                .collect::<Vec<_>>()
                .join(" "),
            String::new()
        )
    }

    #[test]
    fn all_types() {
        let integer = column("integer", INTEGER, []);
        let int = column("int", INT, []);
        let tinyint = column("tinyint", TINYINT, []);
        let smallint = column("smallint", SMALLINT, []);
        let mediumint = column("mediumint", MEDIUMINT, []);
        let bigint = column("bigint", BIGINT, []);
        let unsigned_big_int = column("unsigned_big_int", UNSIGNED_BIG_INT, []);
        let int2 = column("int2", INT2, []);
        let int8 = column("int8", INT8, []);
        let text = column("text", TEXT, []);
        let clob = column("clob", CLOB, []);
        let blob = column("blob", BLOB, []);
        let real = column("real", REAL, []);
        let double = column("double", DOUBLE, []);
        let double_precision = column("double_precision", DOUBLE_PRECISION, []);
        let float = column("float", FLOAT, []);
        let numeric = column("numeric", NUMERIC, []);
        let boolean = column("boolean", BOOLEAN, []);
        let date = column("date", DATE, []);
        let datetime = column("datetime", DATETIME, []);

        struct MyTable {
            columns: Vec<Arc<Column>>,
        }

        impl Table for MyTable {
            fn name(&self) -> &str {
                "my_table"
            }

            fn columns(&self) -> &[Arc<Column>] {
                &self.columns
            }
        }

        let sql = MyTable {
            columns: vec![
                integer.clone(),
                int.clone(),
                tinyint.clone(),
                smallint.clone(),
                mediumint.clone(),
                bigint.clone(),
                unsigned_big_int.clone(),
                int2.clone(),
                int8.clone(),
                text.clone(),
                clob.clone(),
                blob.clone(),
                real.clone(),
                double.clone(),
                double_precision.clone(),
                float.clone(),
                numeric.clone(),
                boolean.clone(),
                date.clone(),
                datetime.clone(),
            ],
        }
        .create_sql();

        assert_eq!(
            sql,
            format!("CREATE TABLE my_table ({} INTEGER, {} INT, {} TINYINT, {} SMALLINT, {} MEDIUMINT, {} BIGINT, {} UNSIGNED BIG INT, {} INT2, {} INT8, {} TEXT, {} CLOB, {} BLOB, {} REAL, {} DOUBLE, {} DOUBLE PRECISION, {} FLOAT, {} NUMERIC, {} BOOLEAN, {} DATE, {} DATETIME)",
                    integer.name(), int.name(), tinyint.name(), smallint.name(), mediumint.name(), bigint.name(), unsigned_big_int.name(), int2.name(), int8.name(), text.name(), clob.name(), blob.name(), real.name(), double.name(), double_precision.name(), float.name(), numeric.name(), boolean.name(), date.name(), datetime.name()
            ));

        rusqlite::Connection::open_in_memory()
            .unwrap()
            .execute(&sql, params![])
            .unwrap();
    }

    #[test]
    fn unique_constraint() {
        struct UniqueTable {
            columns: Vec<Arc<Column>>,
        }

        impl Table for UniqueTable {
            fn name(&self) -> &str {
                "unique_table"
            }

            fn columns(&self) -> &[Arc<Column>] {
                &self.columns
            }
        }

        let col1 = column("val1", TEXT, []);
        let col2 = column("val2", TEXT, []);
        let sql = UniqueTable {
            columns: vec![
                column("id", INTEGER, [PRIMARY_KEY, NOT_NULL]),
                col1.clone(),
                col2.clone(),
                unique([col1, col2]),
            ],
        }
        .create_sql();

        assert_eq!(sql, "CREATE TABLE unique_table (id INTEGER PRIMARY KEY NOT NULL, val1 TEXT, val2 TEXT, UNIQUE (val1, val2))");

        rusqlite::Connection::open_in_memory()
            .unwrap()
            .execute(&sql, params![])
            .unwrap();
    }

    #[test]
    fn primary_constraint() {
        struct PrimaryTable {
            column: Vec<Arc<Column>>,
        }

        impl Table for PrimaryTable {
            fn name(&self) -> &str {
                "primary_table"
            }

            fn columns(&self) -> &[Arc<Column>] {
                &self.column
            }
        }

        let col1 = column("col1", TEXT, []);
        let col2 = column("col2", TEXT, []);
        let sql = PrimaryTable {
            column: vec![col1.clone(), col2.clone(), primary_key([col1, col2])],
        }
        .create_sql();

        assert_eq!(
            sql,
            "CREATE TABLE primary_table (col1 TEXT, col2 TEXT, PRIMARY KEY (col1, col2))"
        );

        rusqlite::Connection::open_in_memory()
            .unwrap()
            .execute(&sql, params![])
            .unwrap();
    }

    #[test]
    fn foreignkey() {
        struct MyTable {
            hoge_column: Arc<Column>,
            columns: Vec<Arc<Column>>,
        }

        impl MyTable {
            fn new() -> Self {
                let hoge_column = column("hoge", TEXT, []);
                Self {
                    hoge_column: hoge_column.clone(),
                    columns: vec![
                        column("id", INTEGER, [PRIMARY_KEY, NOT_NULL]),
                        column("val", TEXT, [DEFAULT("def".into())]),
                        hoge_column.clone(),
                    ],
                }
            }
        }

        impl Table for MyTable {
            fn name(&self) -> &str {
                "my_table"
            }

            fn columns(&self) -> &[Arc<Column>] {
                &self.columns
            }
        }

        let my_table = MyTable::new();

        let mytable_sql = MyTable::new().create_sql();
        assert_eq!(
            mytable_sql,
            "CREATE TABLE my_table (id INTEGER PRIMARY KEY NOT NULL, val TEXT DEFAULT 'def', hoge TEXT)"
        );

        rusqlite::Connection::open_in_memory()
            .unwrap()
            .execute(&mytable_sql, params![])
            .unwrap();

        struct ForeignTable {
            columns: Vec<Arc<Column>>,
        }

        impl ForeignTable {
            fn new(my_table: &MyTable) -> Self {
                let rc_column = column("id", INTEGER, [PRIMARY_KEY, NOT_NULL]);
                Self {
                    columns: vec![
                        rc_column.clone(),
                        foreign_key(
                            rc_column.clone(),
                            REFERENCES,
                            my_table,
                            my_table.hoge_column.clone(),
                            [],
                        ),
                    ],
                }
            }
        }

        impl Table for ForeignTable {
            fn name(&self) -> &str {
                "foreign_table"
            }

            fn columns(&self) -> &[Arc<Column>] {
                &self.columns
            }
        }

        let foreigntable_sql = ForeignTable::new(&my_table).create_sql();
        assert_eq!(
            foreigntable_sql,
            "CREATE TABLE foreign_table (id INTEGER PRIMARY KEY NOT NULL, FOREIGN KEY (id) REFERENCES my_table (hoge))");

        rusqlite::Connection::open_in_memory()
            .unwrap()
            .execute(&foreigntable_sql, params![])
            .unwrap();
    }
}
