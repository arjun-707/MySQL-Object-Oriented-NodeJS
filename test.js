const { LocalhostMysql } = require('./mySqlDB/common.mysql.model');

async function test() {

  const testLocalhost = new LocalhostMysql();
  await testLocalhost.createConnectionPool()
  testLocalhost.enableSession = true; // require to start your transaction
  testLocalhost.table = "admin";
  const data1 = await testLocalhost.fetchResult([], 1, 0) // without condition but with limit and offset
  console.log("data1 -----", data1.length)
  const data2 = await testLocalhost.fetchResult(["name = 'arjun'"]) // with condition
  console.log("data2 ----", data2.length);

  // const set = ["name = 'arjun'"]
  // const condition = ["name = 'singh'"]
  // await testLocalhost.update(set, condition)
  // testLocalhost.sessionCommit(); // required to commit your transaction
  // testLocalhost.sessionRollback(); // on error

  testLocalhost.closePool();
}

test()