const MySqlDB = require("./DB");
const config = require(`./config`);

class LocalhostMysql extends MySqlDB {
  constructor(operation = 'write') {
    super(MySqlDB);
    this.dbConfig = config.mysql.Localhost
    if (operation == 'read')
      this.dbConfig = config.mysql.LocalhostRead
  }
}

module.exports = {
  LocalhostMysql
};