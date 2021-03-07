require('dotenv').config();
const app = require('express')();
const axios = require('axios').default;
const {Queue, Worker} = require('bullmq');
const URL = require('url');
const fs = require('fs');
const path = require('path');
const bodyParser = require('body-parser');

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
    console.log(err.stack);
    throw err;
  });

  await job.updateProgress(100);

  return obj;
}, {concurrency: 20});

app.use(bodyParser.json());

app.post('/import', async (req, res) =>{
  if (req.body.secret !== process.env.IMPORT_SECRET) {
    res.status(401).send('hmmmm');
  }

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

app.post('/generate-list', async (req, res)=>{
  if (req.body.secret !== process.env.IMPORT_SECRET) {
    res.status(401).send('hmmmm');
  }

  const {data: fromData} = await client.get(
      'https://raw.githubusercontent.com/FottyM/spurgeon-gems/master/json/spurgeongems.json', {
        responseType: 'json',
      },
  );

  const gemMap = fromData.reduce(
      (acc, curr) => {
        const uriSplit = curr.uri.split('/');
        const fileName = uriSplit[uriSplit.length - 1];
        const key = `${curr.title} - ${fileName}`;
        return acc.set(key, curr);
      }, new Map(),
  );

  try {
    const params = {
      Bucket: process.env.DO_SPACES_NAME,
    };

    const data = await s3.listObjects(params).promise();
    const list = data['Contents'].map((obj) => ({
      ...gemMap.get(obj['Key']),
      audioTitle: obj['Key'],
      uri: URL.format(`https://${process.env.DO_SPACES_NAME}.${process.env.DO_SPACES_ENDPOINT_CDN}/${obj['Key']}`),
    }));

    const contentDir = path.resolve('json');
    if (!fs.existsSync(path.resolve('json'))) {
      fs.mkdirSync(contentDir);
      fs.writeFileSync(
          path.join(contentDir, 'do.json'), JSON.stringify(list, null, 2),
      );
    }

    res.status(200).json(list);
  } catch (error) {
    console.error(error.stack);
    res.status(500).send(error);
  }
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

