import React from "react";
import * as duckdb from "@duckdb/duckdb-wasm";

function DuckDbClient() {

  const dbRef =React.useRef<duckdb.AsyncDuckDB>(null!);
  const [output, setOutput] = React.useState<string>("");

  const initialize = async () => {
    const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();

    // Select a bundle based on browser checks
    const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

    const worker_url = URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker!}");`], {
        type: "text/javascript",
      })
    );

    // Instantiate the asynchronus version of DuckDB-wasm
    const worker = new Worker(worker_url);
    const logger = new duckdb.ConsoleLogger();
    dbRef.current = new duckdb.AsyncDuckDB(logger, worker);
    await dbRef.current.instantiate(bundle.mainModule, bundle.pthreadWorker);
    URL.revokeObjectURL(worker_url);
    console.log("DuckDB-wasm initialized");
  };

  React.useEffect(() => {    
    initialize();
  }, []);


  const loadData = async () => {
    console.log("Loading data");
    const res = await fetch('http://localhost:3000/1M.parquet');
    await dbRef.current.registerFileBuffer('1M.parquet', new Uint8Array(await res.arrayBuffer()));
    console.log("Loaded data");
    setOutput("Loaded data");
  }

  const enumFiles = async () => {
    console.log("enum files");
    const files = await dbRef.current.globFiles("*");
    console.log(files);
    setOutput(files.join(", "));
  }

  const query = async () => {
    console.log("query");
    const c = await dbRef.current.connect();
    const result = await c.query("SELECT * FROM '1M.parquet' LIMIT 100;");
    console.table([...result.toArray()]);
    setOutput([...result.toArray()].toString());
  }


  return <div className="DuckDbClient">
    <button onClick={loadData}>Load Data</button>
    <button onClick={enumFiles}>enum files</button>
    <button onClick={query}>query</button>
    <br/>
    <div>{output}</div>
  </div>;
}

export default DuckDbClient;
