<b>Polyflow</b> is an ETL tool based on Apache Spark.
<p>Easy configurable from application.conf yaml file, it uses spark sql to transfer data to multiple sinks.</p>

Input sources supported:
<ul>
<li>Spark supported file formats : parquet, json, csv, etc </li>
<li>Kafka</li>
<li>JDBC</li>
<li>Cassandra</li>
<li>GS</li>
</ul>

Output sources supported:
<ul>
<li>Spark supported file formats : parquet, json, csv, etc </li>
<li>Kafka</li>
<li>JDBC</li>
<li>Cassandra</li>
<li>BigQuery</li>
<li>GS</li>
</ul>

<b>Application.conf : </b>
<p>a) Spark = spark conf related settings</p>
<p>b) Job = input part</p>
<p>c) Sink = writing part</p>
<p>d) Transfer = sql for spark sql, data manipulation</p>
<p>e) Context : Batch or Streaming </p>