require("dotenv").config();
const worker = require('worker_threads');
const AWS = require("aws-sdk");
const { json } = require("express");

const senderQueue = process.env.AWS_SENDER_QUEUE_URL;
const sqsRegion = process.env.AWS_SQS_REGION;
const awsAccountId = process.env.AWS_ACCOUNT_ID;
const accessKeyId = process.env.AWS_ACCESS_KEY;
const secretAccessKey = process.env.AWS_SECRET_KEY;
const recieverQueue = process.env.AWS_RECIEVER_QUEUE_URL;

//AWS.config.update({ region: sqsRegion });
//Create SQS client
const sqsClient = new AWS.SQS({ apiVersion: "2012-11-05", region:sqsRegion, accessKeyId, secretAccessKey });

const uploadImageToSQS = (filename, response) => {
// Set the parameters
const params = {
  MessageBody: filename,
  QueueUrl: `https://sqs.${sqsRegion}.amazonaws.com/${awsAccountId}/${senderQueue}` 
};

let senderqueueResponse =  sqsClient.sendMessage(params).promise();
response = {
    statusCode: 200,
    body: senderqueueResponse,
};
return response;
}


const receiveResponseFromSqs = (imageName, response) =>{

const params = {
 QueueUrl: `https://sqs.${sqsRegion}.amazonaws.com/${awsAccountId}/${recieverQueue}`,
 VisibilityTimeout: 10,
 WaitTimeSeconds: 0,
};

sqsClient.receiveMessage(params, (err, data) => {
  if (data.Messages) {
    const imageDetails = JSON.parse(data.Messages[0].Body);
    //if(Object.keys(imageDetails)[0] == imageName){
      var deleteParams = {
        QueueUrl: `https://sqs.${sqsRegion}.amazonaws.com/${awsAccountId}/${recieverQueue}`,
        ReceiptHandle: data.Messages[0].ReceiptHandle,
      };
      sqsClient.deleteMessage(deleteParams, (err, data) => {
        if (err) {
          response.status(400).send({
            message: "Some error occured",
            data,
          });
        } else {
          //console.log(imageDetails);
          response.status(200).send(imageDetails);
        }
          
          });
    /*}
    else {
      return receiveResponseFromSqs(imageName, response);
    }*/
  }
  else if (err) {
    response.status(400).send(
      {
        "Message" : "Some error occured while fetching messages",
        data,
      }
    );
  }
  else{
      return receiveResponseFromSqs(imageName, response);
  }
});
}



const deleteFromSqs = (imageName, response) =>{ 
  var params = {
    QueueUrl: `https://sqs.${sqsRegion}.amazonaws.com/${awsAccountId}/${recieverQueue}`
  };
  
  response =sqsClient.purgeQueue(params).promise();
  return response

}


exports.uploadImageToSQS = uploadImageToSQS;
exports.receiveResponseFromSqs = receiveResponseFromSqs;
exports.deleteFromSqs = deleteFromSqs;
