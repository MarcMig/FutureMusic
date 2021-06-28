// 1. for logs, add cloudwatch permissions to lambda
// 2. for copying, add s3 bucket permissions to lambda: s3:putObject
// 3. p


const { PassThrough } = require('stream');
const AWS = require('aws-sdk');
const superagent = require('superagent');

const BUCKET_NAME = 'sound-scraping';

AWS.config.loadFromPath('./config.json');

const s3 = new AWS.S3();

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

const Lambda = async (event) => {
  const { originUrls } = event;

  const result = await Promise.all(originUrls.map((el) => {
    const { id, name, url } = el;
    let upload = null;
    try {
      // these are the parameters
      const stream = new PassThrough();
      const req = superagent.post(url).set({ 'User-Agent': 'Mozilla/5.0' })
                                      .send("");
      const fileName = `track-files/${id}.wav`;

      const wavMimeType = 'audio/wav';

      // create an upload to s3
      upload = putStreamToS3(s3, {
        Bucket: BUCKET_NAME,
        Key: fileName,
        Metadata: {'name': name},
        ContentType: wavMimeType,
        Body: stream,
      });

      // pipe the request (origin) to the stream (which is the s3 upload's body)
      req.pipe(stream);
      console.log('Piped origin to destination')
    } catch (error) {
      console.error(`Error piping request for ${url}`)
    }

    // if you want to wait for each request, comment out this line below, and uncomment the line after
    return upload;
    // await upload;
    // return true;
  }));

  console.log('Copy results');
  console.log(result);
}

const tracks = {"originUrls":
[{
  id: '1',
  url: 'https://todd38997692451975.googuu.xyz/api/get.php?id=3238778865c5b3773942c7e9871',
  name: 'trial_track',
}]};

Lambda(tracks)