import { config } from "dotenv";
import Kafka from "node-rdkafka";

config({ path: "../../.env" });

console.log(
  `Listening for messages in the '${process.env.DEMO_TOPIC_NAME}' topic`
);

const stream = new Kafka.createReadStream(
  {
    "metadata.broker.list": process.env.BOOTSTRAP_SERVER,
    "group.id": "GROUP_ID",
    "security.protocol": "ssl",
    "ssl.key.location": "../../certs/service.key",
    "ssl.certificate.location": "../../certs/service.cert",
    "ssl.ca.location": "../../certs/ca.pem",
  },
  { "auto.offset.reset": "beginning" },
  { topics: [process.env.DEMO_TOPIC_NAME] }
);

stream.on("data", (message) => {
  console.log("Received message:", message.value.toString());
});
