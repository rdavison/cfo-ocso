from flask import Flask
from flask.ext.restful import reqparse, abort, Api, Resource
import sqlite3
import time
from datetime import datetime, timedelta

app = Flask(__name__)
api = Api(app)

class DbWrapper(object):
    FILENAME = '2014-12-snapshot.sqlite3db'

    def __init__(self):
        self.conn = sqlite3.connect(DbWrapper.FILENAME)

    def __del__(self):
        self.conn.close()

    def execute_query(self, query):
        cursor = self.conn.execute(query)
        return cursor

class Call(Resource, DbWrapper):
    def get(self, callid):
        query = """
            SELECT calls.id, calls.t, calls.latitude, calls.longitude, 
                   calls.address, calls.source, upper(reasons.desc)
            FROM calls
            JOIN reasons ON calls.reason = reasons.id
            WHERE calls.id = {0}
            """.format(callid)
            
        # execute query and get cursor
        cursor = self.execute_query(query)

        # list of column names
        columns = [
            'id',
            'time',
            'latitude',
            'longitude',
            'address',
            'source',
            'reason'
        ]

        # convert cursor to dict
        try:
            row = cursor.fetchone()
            row_dict = {}
            for i, col in enumerate(columns):
                row_dict[col] = row[i]
            row = row_dict
        except:
            row = {}

        return row


class Calls(Resource, DbWrapper):
    ROUTE = '/calls/page'
    def get(self, page_num):
        page_size = 10
        query = """
            SELECT calls.id, calls.t, calls.latitude, calls.longitude,
                   calls.address, calls.source, upper(reasons.desc)
            FROM calls
            JOIN reasons ON calls.reason = reasons.id
            LIMIT {0}, {1}""".format(page_num * page_size, page_size)
            
        # execute query and get cursor
        cursor = self.execute_query(query)

        # list of column names
        columns = [
            'id',
            'time',
            'latitude',
            'longitude',
            'address',
            'source',
            'reason'
        ]

        # list of rows
        rows = []

        # convert cursor to list of dicts
        for row in cursor:
            row_dict = {}
            for i, col in enumerate(columns):
                row_dict[col] = row[i]
            rows.append(row_dict)

        # build final results
        results = {
            'calls': rows,
        }

        # decide whether or not to include 'prev' and 'next' in results
        if page_num > 0 and len(rows) > 0:
            results['prev'] = self.ROUTE + '/' + str(page_num - 1)

        if len(rows) == page_size:
            results['next'] = self.ROUTE + '/' + str(page_num + 1)

        # done
        return results


class CallsByDay(Resource, DbWrapper):
    def get(self, date):
        page_size = 10
        parsed_date = time.strptime(date, '%Y-%m-%d')
        range_start = datetime(*parsed_date[:6])
        range_end = range_start + timedelta(days = 1)
        query = """
            SELECT calls.id, calls.t, calls.latitude, calls.longitude,
                   calls.address, calls.source, upper(reasons.desc)
            FROM calls
            JOIN reasons ON calls.reason = reasons.id
            WHERE calls.t >= '{0}' and calls.t < '{1}'
            ORDER BY calls.t
            """.format(range_start.strftime('%Y-%m-%d'), range_end.strftime('%Y-%m-%d'))
            
        # execute query and get cursor
        cursor = self.execute_query(query)

        # list of column names
        columns = [
            'id',
            'time',
            'latitude',
            'longitude',
            'address',
            'source',
            'reason'
        ]

        # list of rows
        rows = []

        # convert cursor to list of dicts
        for row in cursor:
            row_dict = {}
            for i, col in enumerate(columns):
                row_dict[col] = row[i]
            rows.append(row_dict)

        # build final results
        results = {
            'calls': rows,
        }

        # done
        return results


class Tweet(Resource, DbWrapper):
    def get(self, tweetid):
        query = """
            SELECT tweq.id, tweq.userid, tweq.message, tweq.t,
                   tweq.lat, tweq.long
            FROM tweq
            WHERE tweq.id = {0}
            """.format(tweetid)
            
        # execute query and get cursor
        cursor = self.execute_query(query)

        # list of column names
        columns = [
            'id',
            'userid',
            'message',
            'time',
            'latitude',
            'longitude',
        ]

        # convert cursor to dict
        try:
            row = cursor.fetchone()
            row_dict = {}
            for i, col in enumerate(columns):
                row_dict[col] = row[i]
            row = row_dict
        except:
            row = {}

        return row


class Tweets(Resource, DbWrapper):
    ROUTE = '/tweets/page'
    def get(self, page_num):
        page_size = 10
        query = """
            SELECT tweq.id, tweq.userid, tweq.message, tweq.t,
                   tweq.lat, tweq.long
            FROM tweq
            ORDER BY tweq.id
            LIMIT {0}, {1}""".format(page_num * page_size, page_size)
            
        # execute query and get cursor
        cursor = self.execute_query(query)

        # list of column names
        columns = [
            'id',
            'userid',
            'message',
            'time',
            'latitude',
            'longitude',
        ]

        # list of rows
        rows = []

        # convert cursor to list of dicts
        for row in cursor:
            row_dict = {}
            for i, col in enumerate(columns):
                row_dict[col] = row[i]
            rows.append(row_dict)

        # build final results
        results = {
            'tweets': rows,
        }

        # decide whether or not to include 'prev' and 'next' in results
        if page_num > 0 and len(rows) > 0:
            results['prev'] = self.ROUTE + '/' + str(page_num - 1)

        if len(rows) == page_size:
            results['next'] = self.ROUTE + '/' + str(page_num + 1)

        # done
        return results


class TweetsByDay(Resource, DbWrapper):
    def get(self, date):
        page_size = 10
        parsed_date = time.strptime(date, '%Y-%m-%d')
        range_start = datetime(*parsed_date[:6])
        range_end = range_start + timedelta(days = 1)
        query = """
            SELECT tweq.id, tweq.userid, tweq.message, tweq.t,
                   tweq.lat, tweq.long
            FROM tweq
            WHERE tweq.t >= '{0}' and tweq.t < '{1}'
            ORDER BY tweq.t
            """.format(range_start.strftime('%Y-%m-%d'), range_end.strftime('%Y-%m-%d'))

        print query
            
        # execute query and get cursor
        cursor = self.execute_query(query)

        # list of column names
        columns = [
            'id',
            'userid',
            'message',
            'time',
            'latitude',
            'longitude',
        ]

        # list of rows
        rows = []

        # convert cursor to list of dicts
        for row in cursor:
            row_dict = {}
            for i, col in enumerate(columns):
                row_dict[col] = row[i]
            rows.append(row_dict)

        # build final results
        results = {
            'tweets': rows,
        }

        # done
        return results


## Actually setup the Api resource routing here
api.add_resource(Call, '/call/<int:callid>')
api.add_resource(Calls, '/calls/page/<int:page_num>')
api.add_resource(CallsByDay, '/calls/day/<string:date>')
api.add_resource(Tweet, '/tweet/id/<int:tweetid>')
api.add_resource(Tweets, '/tweets/page/<int:page_num>')
api.add_resource(TweetsByDay, '/tweets/day/<string:date>')

if __name__ == '__main__':
    app.run(debug=True)
