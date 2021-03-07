require('dotenv').config();
const app = require('express')();
const axios = require('axios').default;
const {Queue, Worker} = require('bullmq');
const {s3} = require('./src/do-spaces');

const client = axios.create({
  responseType: 'arraybuffer',
  headers: {
    'Content-Type': 'audio/mpeg',
  },
});
const QUEUE_NAME = 'import';

const importQueue = new Queue(
    QUEUE_NAME, {
      defaultJobOptions: {
        attempts: 5,
      },
    },
);


const importWorker = new Worker(QUEUE_NAME, async (job) => {
  const uriSplit = job.data.uri.split('/');
  const fileName = uriSplit[uriSplit.length - 1];


  const {data} = await client.get(job.data.uri).catch((err) =>{
    throw err;
  });

  const params = {
    Bucket: process.env.DO_SPACES_NAME,
    Key: `${job.data.title} - ${fileName}`,
    Body: data,
    ACL: 'public-read',
  };

  await job.updateProgress(30);

  const obj = await s3.putObject(params).promise().catch((err) =>{
    throw err;
  });

  await job.updateProgress(100);

  return obj;
}, {concurrency: 20});


app.post('/import', async (_, res) =>{
  const {data} = await client.get(
      'https://raw.githubusercontent.com/FottyM/spurgeon-gems/master/json/spurgeongems.json', {
        responseType: 'json',
      },
  );

  try {
    const jobs = data.map((d) => importQueue.add(QUEUE_NAME, d));
    await Promise.all(jobs);
  } catch (error) {
    console.log(error);
    res.send(error).status(500);
  }
  res.status(200).send('import started...');
});

app.all('*', (_, res) =>{
  res.status(404).send('not found');
});

app.listen(3000, ()=>{
  console.log('listening on http://localhost:3000');
});

importWorker.on('progress', (job, progress) => {
  console.log('in progress', job.data.title, progress);
});

importWorker.on('completed', (job, returnvalue) => {
  console.log('completed', job.data.title, returnvalue);
});

