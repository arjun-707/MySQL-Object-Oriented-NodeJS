const mysql = require("mysql");
const { promisify } = require("util");
class MySqlDB {
  constructor(table = "", fields = [" * "], groupBy = "", orderby = "") {
    this.connect = false;
    this.table = table;
    this.fields = fields;
    this.groupBy = groupBy;
    this.orderby = orderby;
    this.connectPool = false;
    this.connectionPoolCount = 0
    this.enableSession = false
  }
  createConnection() {
    if (this.connect) return this.connect;
    let __this = this;
    return new Promise((resolve, reject) => {
      if (__this.dbConfig.connectionLimit)
        delete __this.dbConfig.connectionLimit;
      __this.connect = mysql.createConnection(__this.dbConfig);
      __this.connect.connect(async (err) => {
        if (err) return reject(err);
        else {
          console.log(`Mysql DB ${__this.dbConfig.database} Connected!`);
          try {
            if (__this.enableSession) await __this.sessionStart()
          }
          catch (err) {
            return reject(err)
          }
          return resolve(__this.connect);
        }
      });
      __this.connect.on('error', console.error)
    });
  }
  createConnectionPool() {
    console.log('********************* createConnectionPool *********************')
    if (this.connectPool) return this.connectPool;
    let __this = this;
    return new Promise((resolve, reject) => {
      try {
        __this.dbConfig.connectionLimit = 100;
        __this.connectPool = mysql.createPool(__this.dbConfig);
        __this.connectPool.on("enqueue", function() {
          console.log("MySql Pool Waiting for available connection slot");
        });
        __this.connectPool.on("release", function (connection) {
          __this.connect = false;
          console.log(`MySql Pool Connection ${connection.threadId} released`);
          console.log(`MySql Pool connected after release: ${--__this.connectionPoolCount}`);
        });
        return resolve(__this.connectPool);
      }
      catch (err) {
        return reject(err)
      }
    });
  }
  async getConnectionFromPool() {
    console.log('********************* getConnectionFromPool *********************')
    let __this = this
    return new Promise((resolve, reject) => {
      if (__this.connectPool) {
        __this.connectPool.getConnection(function (err, connection) {
          if (err) {
            return reject(err)
          }
          console.log(`MySql Pool connected : ${++__this.connectionPoolCount}`);
          return resolve(connection)
        })
      }
      else {
        return reject('pool connection is disabled')
      }
    })
  }
  async sessionStart() {
    console.log("********************* sessionStart *********************");
    if (this.connect) {
      if (this.enableSession) {
        const beginTransaction = promisify(this.connect.beginTransaction).bind(this.connect);
        try {
          await beginTransaction();
        }
        catch (err) {
          throw new Error(err.message);
        }
      }
      else 
        throw new Error("enable session");
    }
    else
      throw new Error("enable session");
  }
  async sessionCommit() {
    console.log("********************* sessionCommit *********************");
    if (this.connect) {
      if (this.enableSession) {
        const commit = promisify(this.connect.commit).bind(this.connect);
        try {
          await commit();
          if (this.connectPool)
            this.connect.release()
        }
        catch (err) {
          try {
            await this.sessionRollback();
            if (this.connectPool)
              this.connect.release()
            throw new Error(err.message);
          }
          catch (err) {
            throw new Error(err.message);
          }
        }
      }
      else 
        throw new Error("enable session");
    }
    else
      throw new Error("enable session");
  }
  async sessionRollback() {
    console.log("********************* sessionRollback *********************");
    if (this.connect) {
      if (this.enableSession) {
        const rollback = promisify(this.connect.rollback).bind(this.connect);
        return rollback();
      }
      else 
        throw new Error("enable session");
    }
    else
      throw new Error("enable session");
  }
  async checkTableExist() {
    if (typeof this.table != "string" || this.table.length < 1)
      throw new Error("invalid table name");
    try {
      let exist = await this.executeQuery(
        `SELECT COUNT(1) AS exist FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '${this.dbConfig.database}' and TABLE_NAME = '${this.table}'`
      );
      if (Array.isArray(exist) && exist.length > 0)
        return parseInt(exist[0].exist);
      return 0;
    } catch (e) {
      console.log("TABLE EXIST query exception", e);
      return new Promise((rs, rj) => rj(e));
    }
  }
  async getTableSchema() {
    if (typeof this.table != "string" || this.table.length < 1)
      throw new Error("invalid table name");
    let tableColumns = [];
    try {
      let tableSchema = await this.executeQuery(`DESC ${this.table}`);
      if (Array.isArray(tableSchema) && tableSchema.length > 0) {
        for (let i = 0; i < tableSchema.length; i++) {
          let temp = {};
          let type = tableSchema[i].Type.split("(")[0];
          temp[tableSchema[i].Field] = (type.indexOf("int") > -1 ? "int" : (type.indexOf("bool") > -1 ? "bool" : "string"));
          tableColumns.push(temp);
        }
      }
      return tableColumns;
    } catch (e) {
      console.log("TABLE SCHEMA query exception", e);
      return new Promise((rs, rj) => rj(e));
    }
  }
  async listTableNames() {
    let tableList = [];
    try {
      let list = await this.executeQuery(`SHOW TABLES`);
      if (Array.isArray(list) && list.length > 0) {
        for (let i = 0; i < list.length; i++) {
          tableList.push(list[i][`Tables_in_${this.dbName}`]);
        }
      }
      return tableList;
    } catch (e) {
      console.log("TABLE LIST query exception", e);
      return new Promise((rs, rj) => rj(e));
    }
  }
  countQuery(condition) {
    if (typeof this.table != "string" || this.table.length < 1)
      throw new Error("invalid table name");
    let where = "";
    if (condition.length > 0) where = " WHERE " + condition.join(" AND ");
    try {
      return `SELECT COUNT(*) AS total FROM ${this.table} ${where} ${this.groupBy} `;
    } catch (e) {
      console.log("query exception", e);
      return new Promise((rs, rj) => rj(e));
    }
  }
  dataQuery(condition, limit = "limit", offset = "offset") {
    if (typeof this.table != "string" || this.table.length < 1)
      throw new Error("invalid table name");
    let where = "";
    if (condition.length > 0) where = " WHERE " + condition.join(" AND ");
    try {
      let extra = "";
      if (!isNaN(limit)) extra = " LIMIT " + limit;
      if (!isNaN(offset)) extra += " OFFSET " + offset;
      return `SELECT ${this.fields} FROM ${this.table} ${where} ${this.groupBy} ${this.orderby} ${extra}`;
    } catch (e) {
      console.log("query exception", e);
      return new Promise((rs, rj) => rj(e));
    }
  }
  async fetchResultCount(condition = []) {
    if (typeof this.table != "string" || this.table.length < 1)
      throw new Error("invalid table name");
    let where = "";
    if (condition.length > 0) where = " WHERE " + condition.join(" AND ");
    try {
      let count = await this.executeQuery(
        `SELECT COUNT(*) AS total FROM ${this.table} ${where} ${this.groupBy} `
      );
      // console.log('count ======>', count)
      if (Array.isArray(count) && count.length > 0)
        return parseInt(count[0].total);
      return 0;
    } catch (e) {
      console.log("COUNT query exception", e);
      throw new Error(e);
    }
  }
  async fetchResult(condition = [], limit = "false", offset = "false") {
    console.log("********************* fetchResult *********************");
    if (typeof this.table != "string" || this.table.length < 1)
      throw new Error("invalid table name");
    let where = "";
    if (condition.length > 0) where = " WHERE " + condition.join(" AND ");
    try {
      return await this.executeQuery(
        `SELECT ${this.fields} FROM ${this.table} ${where} ${this.groupBy} ${this.orderby}`,
        limit,
        offset
      );
    } catch (e) {
      console.log("FETCH query exception", e);
      throw new Error(e);
    }
  }
  async insert(rows = []) {
    if (typeof this.table != "string" || this.table.length < 1)
      throw new Error("invalid table name");
    if (!Array.isArray(rows) && rows.length < 1)
      throw new Error("`rows` variable should be array and must have values");
    this.operation = "write";
    try {
      return await this.executeQuery(
        `INSERT INTO ${this.table} (${this.fields}) VALUES ${rows.join(", ")}`
      );
    } catch (e) {
      throw new Error(e);
    }
  }
  async update(set = [], condition = []) {
    if (typeof this.table != "string" || this.table.length < 1)
      throw new Error("invalid table name");
    if (!Array.isArray(set) && set.length < 1)
      throw new Error("`set` variable should be array and must have values");
    let where = "";
    if (condition.length > 0) where = " WHERE " + condition.join(" AND ");
    this.operation = "write";
    try {
      return await this.executeQuery(
        `UPDATE ${this.table} SET ${set.join(", ")} ${where}`
      );
    } catch (e) {
      throw new Error(e);
    }
  }
  executeQuery(query, limit = "string", offset = "string") {
    console.log('********************* executeQuery *********************')
    let __this = this;
    return new Promise(async (resolve, reject) => {
      let extra = "";
      if (!isNaN(limit)) extra = " LIMIT " + limit;
      if (!isNaN(offset)) extra += " OFFSET " + offset;
      console.log(query + extra, __this.enableSession);
      if (!__this.connectPool) {
        try {
          await __this.createConnection();
        }
        catch (err) {
          return reject(err)
        }
        if (!__this.connect) return reject(`Mysql connection pool/connect didn't establish`);
        try {
          __this.connect.query(query + extra, [], function (err, rows, columns) {
            if (!__this.enableSession)
              __this.closeConnection();
            try {
              if (err) {
                console.log(err);
                return reject(err);
              }
              return resolve(rows);
            } catch (ex) {
              console.error("MySQL Exception (query)", ex);
              return reject(ex);
            }
          });
        } catch (ex) {
          console.error("MySQL Exception (query)", ex);
          return reject(ex);
        }
      }
      else {
        try {
          let sessionExists = false
          if (!__this.connect) {
            __this.connect = await __this.getConnectionFromPool()
          }
          else sessionExists = true
          try {
            if (__this.enableSession && !sessionExists) await __this.sessionStart()
            if (__this.connect) {
              __this.connect.query(query + extra, [], function (err, rows, columns) {
                try {
                  if (!__this.enableSession)
                    __this.connect.release();
                  if (err) {
                    console.log(err);
                    return reject(err);
                  }
                  return resolve(rows);
                } catch (ex) {
                  console.error("MySQL Pool Exception (query)", ex);
                  return reject(ex);
                }
              });
            }
            else
              reject('pool closed')
          } catch (ex) {
            console.error("MySQL Pool Exception (query)", ex);
            return reject(ex);
          }
        } catch (ex) {
          console.error("MySQL Pool Exception (query)", ex);
          return reject(ex);
        }
      }
    });
  }
  async closePool() {
    if (!this.connectPool)
      return false
    let __this = this
    try {
      this.connectPool.end(function(err) {
        if (err) console.error(new Error(err));
        __this.connectPool = false;
        console.log("********************* closePool *********************");
      });
    } catch (Ex) {
      console.error(Ex);
    }
  }
  async closeConnection() {
    if (!this.connect)
      return false
    let __this = this;
    try {
      this.connect.end(function(err) {
        if (err) console.error(new Error(err));
        __this.connect = false;
        console.info("MySQL connection closed");
      });
    } catch (Ex) {
      console.error(Ex);
    }
  }
}
module.exports = MySqlDB