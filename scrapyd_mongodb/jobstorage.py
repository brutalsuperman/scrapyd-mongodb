from configparser import NoOptionError

import pymongo
from scrapyd.interfaces import IJobStorage
from scrapyd.jobstorage import Job
from zope.interface import implementer


@implementer(IJobStorage)
class MongoDBJobStorage(object):

    def __init__(self, config, collection='finished_jobs'):
        database_name = config.get('mongodb_name', 'scrapyd_mongodb')
        database_host = config.get('mongodb_host', 'localhost')
        database_port = config.getint('mongodb_port', 27017)
        database_user = self.get_optional_config(config, 'mongodb_user')
        database_pwd = self.get_optional_config(config, 'mongodb_pass')

        if database_user and database_pwd:
            conn_str = (
                'mongodb://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}'
            ).format(
                db_user=database_user,
                db_pwd=database_pwd,
                db_host=database_host,
                db_port=database_port,
                db_name=database_name,
            )
            self.conn = pymongo.MongoClient(conn_str)
        else:
            self.conn = pymongo.MongoClient(
                host=database_host,
                port=database_port,
            )

        self.collection = self.conn.get_database(database_name)[collection]

    @staticmethod
    def get_optional_config(config, name):
        try:
            return config.get(name).replace('\'', '').replace('"', '')
        except NoOptionError:
            return None

    def add(self, job):
        args = {
            'project': job.project, 'spider': job.spider, 'job': job.job,
            'start_time': job.start_time, 'end_time': job.end_time}

        self.collection.insert_one(args)

    def clear(self, finished_to_keep=None):
        if finished_to_keep:
            limit = len(self) - finished_to_keep
            if limit <= 0:
                return  # nothing to delete

            removedIdsArray = [x.get('_id') for x in list(self.collection.find({}, {'_id': 1}).limit(limit).sort({'end_time': -1}))]
            self.collection.remove({'_id': {'$in': removedIdsArray}})

    def __len__(self):
        return self.collection.count_documents({})

    def __iter__(self):
        for j in self.collection.find():
            yield Job(project=j['project'], spider=j['spider'], job=j['job'],
                      start_time=j['start_time'], end_time=j['end_time'])
