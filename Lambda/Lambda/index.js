// 1. for logs, add cloudwatch permissions to lambda
// 2. for copying, add s3 bucket permissions to lambda: s3:putObject
// 3. p

<<<<<<< HEAD

=======
const fs = require('fs');
const path = require('path');
>>>>>>> 3add47360614e6b74750ed7eb57c805bf28eb447
const { PassThrough } = require('stream');
const AWS = require('aws-sdk');
const superagent = require('superagent');

const BUCKET_NAME = 'sound-scraping';

<<<<<<< HEAD
AWS.config.update({ region: process.env.AWS_REGION });

const s3 = new AWS.S3();
=======
AWS.config.loadFromPath('./config.json');
AWS.config.update({region: 'us-east-1'});

const s3 = new AWS.S3({
  apiVersion: '2006-03-01',
  signatureVersion: 'v4',
});
>>>>>>> 3add47360614e6b74750ed7eb57c805bf28eb447

const putStreamToS3 = async (s3Service, { Bucket, Key, ContentType, Metadata, Body }) => {
  console.log(`[AWSUtils.putStreamToS3] Putting ${Key} as stream into ${Bucket}`);
  return s3Service
    .upload({
      Bucket,
      Key,
      ContentType,
      Metadata,
      Body,
    })
    .promise();
};

<<<<<<< HEAD
exports.handler = async (event) => {
  const { originUrls } = event;

  const result = await Promise.all(originUrls.map((el) => {
    const { id, name, url } = el;
=======
const Lambda = async (jsonFileName) => {
  //const { originUrls } = event;
  let trax = null;

  try {
    trax = JSON.parse(fs.readFileSync(jsonFileName));
  } catch (e) {
    console.error(e);
    return;
  }

  const result = await Promise.all(Object.keys(trax).map((key) => {
    const el = trax[key];
    const { track_id, download_url, artist_id, ...rest } = el;
>>>>>>> 3add47360614e6b74750ed7eb57c805bf28eb447
    let upload = null;
    try {
      // these are the parameters
      const stream = new PassThrough();
<<<<<<< HEAD
      const req = superagent.post(url).set({ 'User-Agent': 'Mozilla/5.0' })
                                      .send("");
      const fileName = `track-files/${id}.wav`;
=======
      const req = superagent.post(download_url).set({ 'User-Agent': 'Mozilla/5.0' })
                                      .send("");
      const fileName = `track-files/${track_id}.wav`;
>>>>>>> 3add47360614e6b74750ed7eb57c805bf28eb447

      const wavMimeType = 'audio/wav';

      // create an upload to s3
      upload = putStreamToS3(s3, {
        Bucket: BUCKET_NAME,
        Key: fileName,
<<<<<<< HEAD
        Metadata: {'name': name},
=======
        //Metadata: { ...rest, artist_id: `${artist_id}` },
>>>>>>> 3add47360614e6b74750ed7eb57c805bf28eb447
        ContentType: wavMimeType,
        Body: stream,
      });

      // pipe the request (origin) to the stream (which is the s3 upload's body)
      req.pipe(stream);
      console.log('Piped origin to destination')
    } catch (error) {
<<<<<<< HEAD
      console.error(`Error piping request for ${url}`)
=======
      console.error(`Error piping request for ${download_url}`)
>>>>>>> 3add47360614e6b74750ed7eb57c805bf28eb447
    }

    // if you want to wait for each request, comment out this line below, and uncomment the line after
    return upload;
    // await upload;
    // return true;
  }));

  console.log('Copy results');
  console.log(result);
<<<<<<< HEAD
}
=======
}

// const tracks = {"originUrls":
// [{
//   id: '1',
//   url: 'https://todd38997692451975.googuu.xyz/api/get.php?id=3238778865c5b3773942c7e9871',
//   name: 'trial_track',
// }]};

Lambda(path.join(process.cwd(), 'track_urls_10.json'));
>>>>>>> 3add47360614e6b74750ed7eb57c805bf28eb447
