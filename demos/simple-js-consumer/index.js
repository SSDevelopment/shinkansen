import { config } from "dotenv";
import Kafka from "node-rdkafka";
import { diff } from "deep-object-diff";

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
  { "auto.offset.reset": "latest" },
  { topics: [process.env.DEMO_TOPIC_NAME] }
);

stream.on("data", (message) => {
  const messageValue = JSON.parse(message.value.toString());
  if (messageValue.Event_Type__c === "RECORD_UPDATE") {
    const parsedBody = JSON.parse(messageValue.Body__c);
    console.log(diff(parsedBody.oldObj, parsedBody.newObj));
  }
});
