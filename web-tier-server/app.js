// we use express and multer libraries to send images
const express = require("express");
const multer = require("multer");
const fs = require('fs');
const server = express();
const PORT = 3000;

// uploaded images are saved in the folder "/upload_images"
const upload = multer({ dest: __dirname + "/upload_images" });

const { uploadImage } = require("./s3");
const{ uploadImageToSQS } = require("./sqs");
const{receiveResponseFromSqs} = require("./sqs");
const{deleteFromSqs} = require("./sqs");

server.use(express.static("public"));

server.get('/delete', function(req, res) {
  console.log("In Deletion loop");
  deleteFromSqs(req);
  res.send({status:200});
});

// "myfile" is the key of the http payload
server.post("/uploadImage", upload.single("myfile"), async(req, res) => {
  const s3result = await uploadImage(req.file);
  if (s3result) {
    const sqsResult = await uploadImageToSQS(req.file.originalname, res);
  
    if (sqsResult != undefined) {
      await receiveResponseFromSqs(req.file.originalname, res);
  }
}
});

// You need to configure node.js to listen on 0.0.0.0 so it will be able to accept connections on all the IPs of your machine
const hostname = "0.0.0.0";
server.listen(PORT, hostname, () => {
  console.log(`Server running at http://${hostname}:${PORT}/`);
});
