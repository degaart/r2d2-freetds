pub use freetds;
pub use freetds::{
    Connection,
    ResultSet,
    ColumnId,
    Error,
    error::Type,
    null::Null,
    to_sql::ToSql,
    Statement,
    ParamValue
};
pub use r2d2;
use r2d2::ManageConnection;

#[derive(Debug, Clone)]
pub struct FreetdsConnectionManager {
    pub host: String,
    pub username: String,
    pub password: String,
    pub database: String
}

impl FreetdsConnectionManager {
    pub fn new(
        host: impl AsRef<str>,
        username: impl AsRef<str>,
        password: impl AsRef<str>,
        database: impl AsRef<str>
    ) -> Self {
        Self {
            host: host.as_ref().to_string(),
            username: username.as_ref().to_string(),
            password: password.as_ref().to_string(),
            database: database.as_ref().to_string()
        }
    }
}

impl ManageConnection for FreetdsConnectionManager {
    type Connection = freetds::Connection;
    type Error = freetds::Error;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let mut conn = Connection::new();
        conn.set_client_charset("UTF-8").unwrap();
        conn.set_username(&self.username).unwrap();
        conn.set_password(&self.password).unwrap();
        conn.set_database(&self.database).unwrap();
        conn.set_tds_version_50().unwrap();
        conn.set_login_timeout(5).unwrap();
        conn.set_timeout(5).unwrap();
        conn.connect(&self.host)?;
        Ok(conn)
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        let db_name = conn.db_name()?;
        if db_name != self.database {
            conn.execute(&format!("use {}", self.database), &[])?;
        }
        Ok(())
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        !conn.is_connected()
    }

}

#[cfg(test)]
mod tests {
    use std::{time::Duration, thread};
    use crate::FreetdsConnectionManager;

    const SERVER: &str = "192.168.130.221:2025";

    #[test]
    fn test_freetds_connection_manager() {
        let manager = FreetdsConnectionManager::new(
            SERVER,
            "sa",
            "",
            "master");
        let pool = r2d2::Pool::builder()
            .max_size(1)
            .max_lifetime(Some(Duration::from_secs(5)))
            .build(manager)
            .unwrap();

        let mut handles = Vec::new();
        for i in 0..15 {
            let pool = pool.clone();
            let handle = thread::spawn(move || {
                let mut conn = pool.get().unwrap();
                let mut rs = conn.execute("select getdate()", &[]).unwrap();
                while rs.next() {
                    println!("[{}] {}", i, rs.get_string(0).unwrap().unwrap());
                }
                thread::sleep(Duration::from_millis(1000));
            });
            handles.push(handle);
        }

        while let Some(handle) = handles.pop() {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_is_valid() {
        /* The connection should restore current database */
        let manager = FreetdsConnectionManager::new(
            SERVER,
            "sa",
            "",
            "master");
        let pool = r2d2::Pool::builder()
            .max_size(1)
            .max_lifetime(Some(Duration::from_secs(5)))
            .build(manager)
            .unwrap();
        
        let mut conn = pool.get().unwrap();
        conn.execute("use sybsystemprocs", &[]).unwrap();
        let mut rs = conn.execute("select db_name()", &[]).unwrap();
        assert!(rs.next());
        assert_eq!(Some(String::from("sybsystemprocs")), rs.get_string(0).unwrap());
        drop(conn);

        let mut conn = pool.get().unwrap();
        let mut rs = conn.execute("select db_name()", &[]).unwrap();
        assert!(rs.next());
        assert_eq!(Some(String::from("master")), rs.get_string(0).unwrap());
    }

}
