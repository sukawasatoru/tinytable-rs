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

use std::rc::Rc;

pub fn column<T, A>(name: T, column_type: Type, attributes: A) -> Rc<Column>
where
    T: Into<String>,
    A: Into<Vec<Attribute>>,
{
    let attributes = match attributes.into() {
        data if data.is_empty() => None,
        data => Some(data),
    };
    Rc::new(Column::Column {
        name: name.into(),
        column_type,
        attributes,
    })
}

pub fn primary_key<K: AsRef<[Rc<Column>]>>(keys: K) -> Rc<Column> {
    Rc::new(Column::Constraint(format!(
        "{} ({})",
        Attribute::PRIMARY_KEY.name(),
        keys.as_ref()
            .iter()
            .map(|data| data.name().to_owned())
            .collect::<Vec<_>>()
            .join(", ")
    )))
}

pub fn foreign_key<T: Into<String>>(
    column_name: Rc<Column>,
    other_table_name: T,
    other_table_column: Rc<Column>,
) -> Rc<Column> {
    Rc::new(Column::Constraint(format!(
        "FOREIGN KEY ({}) REFERENCES {} ({})",
        column_name.name(),
        other_table_name.into(),
        other_table_column.name()
    )))
}

pub fn unique<K: AsRef<[Rc<Column>]>>(keys: K) -> Rc<Column> {
    Rc::new(Column::Constraint(format!(
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

    fn columns(&self) -> &[Rc<Column>];

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

#[cfg(test)]
mod tests {
    use crate::Attribute::{DEFAULT, NOT_NULL, PRIMARY_KEY};
    use crate::Type::{INTEGER, TEXT};
    use crate::{column, foreign_key, unique, Column, Table};
    use std::rc::Rc;

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
    fn my_database() {
        struct MyTable {
            hoge_column: Rc<Column>,
            columns: Vec<Rc<Column>>,
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
                        unique([hoge_column]),
                    ],
                }
            }
        }

        impl Table for MyTable {
            fn name(&self) -> &str {
                "my-table"
            }

            fn columns(&self) -> &[Rc<Column>] {
                &self.columns
            }
        }

        let my_table = MyTable::new();

        assert_eq!(
            MyTable::new().create_sql(),
            "CREATE TABLE my-table (id INTEGER PRIMARY KEY NOT NULL, val TEXT DEFAULT 'def', hoge TEXT, UNIQUE (hoge))"
        );

        struct ForeignTable {
            columns: Vec<Rc<Column>>,
        }

        impl ForeignTable {
            fn new(my_table: &MyTable) -> Self {
                let rc_column = column("id", INTEGER, [PRIMARY_KEY, NOT_NULL]);
                Self {
                    columns: vec![
                        rc_column.clone(),
                        foreign_key(
                            rc_column.clone(),
                            my_table.name(),
                            my_table.hoge_column.clone(),
                        ),
                    ],
                }
            }
        }

        impl Table for ForeignTable {
            fn name(&self) -> &str {
                "foreign-table"
            }

            fn columns(&self) -> &[Rc<Column>] {
                &self.columns
            }
        }

        assert_eq!(
            ForeignTable::new(&my_table).create_sql(),
            "CREATE TABLE foreign-table (id INTEGER PRIMARY KEY NOT NULL, FOREIGN KEY (id) REFERENCES my-table (hoge))")
    }
}
