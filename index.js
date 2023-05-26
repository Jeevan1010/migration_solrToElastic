const { Client } = require("@elastic/elasticsearch");
const schedule = require("node-schedule");
const _ = require("lodash");
require("dotenv").config();
const solr = require("solr-client");

const elasticClient = new Client({ node: process.env.elasticUri });

const solrClient = solr.createClient({
  host: process.env.solrHost,
  port: process.env.solrPort,
  core: process.env.solrCollection,
  protocol: "http",
});

const migrateSolrToElastic = async () => {
  try {
    const indexResponse = await elasticClient.indices.exists({
      index: process.env.elasticIndex,
    });
    if (!indexResponse) {
      await elasticClient.indices.create({ index: process.env.elasticIndex });
      console.log(
        `${process.env.elasticIndex} index created Successfully in Elastic Search!!`
      );
    }
    const query = solrClient.query().q("-estatus:*").rows(100);
    const solrSearchResult = await solrClient.search(query);
    console.log("Query result:", solrSearchResult.response.docs.length);
    if (!solrSearchResult?.response?.docs.length) {
      return;
    }
    const operations = _.flatMap(solrSearchResult?.response?.docs, (doc) => [
      { index: { _index: process.env.elasticIndex, _id: doc.id } },
      doc,
    ]);
    const bulkResponse = await elasticClient.bulk({
      refresh: true,
      operations,
    });
    await elasticClient.indices.refresh({ index: process.env.elasticIndex });
    if (!bulkResponse.errors) {
      let result = solrSearchResult?.response?.docs.map((data) => {
        delete data._version_;
        data.estatus = { set: "9" };
      });
      await Promise.all(result).then(async (data)=>{
        await solrClient.update(solrSearchResult?.response?.docs);
        solrClient
          .commit()
          .then((res) => console.log("Data Updated successfully!!"));
      })
    }
    const count = await elasticClient.count({
      index: process.env.elasticIndex,
    });
    console.log(count);
  } catch (error) {
    console.log("Error in migrateSolrToElastic", error);
  }
};

solrClient.ping().then((res) => {
  if ((res.status = "OK")) {
    schedule.scheduleJob('*/1 * * * * *', migrateSolrToElastic);
  }
});
