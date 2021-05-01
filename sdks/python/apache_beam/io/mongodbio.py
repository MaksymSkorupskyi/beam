#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""This module implements IO classes to read and write data on MongoDB.


Read from MongoDB
-----------------
:class:`ReadFromMongoDB` is a ``PTransform`` that reads from a configured
MongoDB source and returns a ``PCollection`` of dict representing MongoDB
documents.
To configure MongoDB source, the URI to connect to MongoDB server, database
name, collection name needs to be provided.

Example usage::

  pipeline | ReadFromMongoDB(uri='mongodb://localhost:27017',
                             db='testdb',
                             coll='input')

To read from MongoDB Atlas, use ``bucket_auto`` option to enable
``@bucketAuto`` MongoDB aggregation instead of ``splitVector``
command which is a high-privilege function that cannot be assigned
to any user in Atlas.

Example usage::

  pipeline | ReadFromMongoDB(uri='mongodb+srv://user:pwd@cluster0.mongodb.net',
                             db='testdb',
                             coll='input',
                             bucket_auto=True)


Write to MongoDB:
-----------------
:class:`WriteToMongoDB` is a ``PTransform`` that writes MongoDB documents to
configured sink, and the write is conducted through a mongodb bulk_write of
``ReplaceOne`` operations. If the document's _id field already existed in the
MongoDB collection, it results in an overwrite, otherwise, a new document
will be inserted.

Example usage::

  pipeline | WriteToMongoDB(uri='mongodb://localhost:27017',
                            db='testdb',
                            coll='output',
                            batch_size=10)


No backward compatibility guarantees. Everything in this module is experimental.
"""

# pytype: skip-file

import codecs
import itertools
import json
import logging
import math
import struct
from typing import Union

import apache_beam as beam
from apache_beam.io import iobase
from apache_beam.io.range_trackers import OrderedPositionRangeTracker
from apache_beam.transforms import DoFn
from apache_beam.transforms import PTransform
from apache_beam.transforms import Reshuffle
from apache_beam.utils.annotations import experimental

_LOGGER = logging.getLogger(__name__)

try:
  # Mongodb has its own bundled bson, which is not compatible with bson package.
  # (https://github.com/py-bson/bson/issues/82). Try to import objectid and if
  # it fails because bson package is installed, MongoDB IO will not work but at
  # least rest of the SDK will work.
  from bson import objectid
  from bson import json_util
  from bson.objectid import ObjectId

  # pymongo also internally depends on bson.
  from pymongo import ASCENDING
  from pymongo import DESCENDING
  from pymongo import MongoClient
  from pymongo import ReplaceOne
except ImportError:
  objectid = None
  json_util = None
  ObjectId = None
  ASCENDING = 1
  DESCENDING = -1
  MongoClient = None
  ReplaceOne = None
  _LOGGER.warning("Could not find a compatible bson package.")

__all__ = ["ReadFromMongoDB", "WriteToMongoDB"]


@experimental()
class ReadFromMongoDB(PTransform):
  """A ``PTransform`` to read MongoDB documents into a ``PCollection``."""
  def __init__(
      self,
      uri="mongodb://localhost:27017",
      db=None,
      coll=None,
      filter=None,
      projection=None,
      extra_client_params=None,
      bucket_auto=False,
  ):
    """Initialize a :class:`ReadFromMongoDB`

    Args:
      uri (str): The MongoDB connection string following the URI format.
      db (str): The MongoDB database name.
      coll (str): The MongoDB collection name.
      filter: A `bson.SON
        <https://api.mongodb.com/python/current/api/bson/son.html>`_ object
        specifying elements which must be present for a document to be included
        in the result set.
      projection: A list of field names that should be returned in the result
        set or a dict specifying the fields to include or exclude.
      extra_client_params(dict): Optional `MongoClient
        <https://api.mongodb.com/python/current/api/pymongo/mongo_client.html>`_
        parameters.
      bucket_auto (bool): If :data:`True`, use MongoDB `$bucketAuto` aggregation
        to split collection into bundles instead of `splitVector` command,
        which does not work with MongoDB Atlas.
        If :data:`False` (the default), use `splitVector` command for bundling.

    Returns:
      :class:`~apache_beam.transforms.ptransform.PTransform`

    """
    if extra_client_params is None:
      extra_client_params = {}
    if not isinstance(db, str):
      raise ValueError("ReadFromMongDB db param must be specified as a string")
    if not isinstance(coll, str):
      raise ValueError(
          "ReadFromMongDB coll param must be specified as a string")
    self._mongo_source = _BoundedMongoSource(
        uri=uri,
        db=db,
        coll=coll,
        filter=filter,
        projection=projection,
        extra_client_params=extra_client_params,
        bucket_auto=bucket_auto,
    )

  def expand(self, pcoll):
    return pcoll | iobase.Read(self._mongo_source)


class _BoundedMongoSource(iobase.BoundedSource):
  """A MongoDB source that reads a finite amount of input records.

  This class defines following operations which can be used to read the source
  efficiently.

  * Size estimation - method ``estimate_size()`` may return an accurate
    estimation in bytes for the size of the source.
  * Splitting into bundles of a given size - method ``split()`` can be used to
    split the source into a set of sub-sources (bundles) based on a desired
    bundle size.
  * Getting a MongoDBRangeTracker - method ``get_range_tracker()``
    should return a ``MongoDBRangeTracker`` object for a given position range
    for the position type of the records returned by the source.
  * Reading the data - method ``read()`` can be used to read data from the
    source while respecting the boundaries defined by a given
    ``MongoDBRangeTracker``.

  A runner will perform reading the source in two steps.

  (1) Method ``get_range_tracker()`` will be invoked with start and end
      positions to obtain a ``MongoDBRangeTracker`` for the range of positions
      the runner intends to read.
      Source must define a default initial start and end position range.
      These positions must be used if the start and/or end positions
      passed to the method ``get_range_tracker()`` are ``None``.
  (2) Method read() will be invoked with the ``MongoDBRangeTracker``
      obtained in the previous step.

  **Mutability**

  A ``_BoundedMongoSource`` object should not be mutated while
  its methods (for example, ``read()``) are being invoked by a runner. Runner
  implementations may invoke methods of ``_BoundedMongoSource`` objects through
  multi-threaded and/or reentrant execution modes.
  """
  def __init__(
      self,
      uri=None,
      db=None,
      coll=None,
      filter=None,
      projection=None,
      extra_client_params=None,
      bucket_auto=False,
  ):
    if extra_client_params is None:
      extra_client_params = {}
    if filter is None:
      filter = {}
    self.uri = uri
    self.db = db
    self.coll = coll
    self.filter = filter
    self.projection = projection
    self.spec = extra_client_params
    self.bucket_auto = bucket_auto

  def estimate_size(self):
    with MongoClient(self.uri, **self.spec) as client:
      return client[self.db].command("collstats", self.coll).get("size")

  def _estimate_average_document_size(self):
    with MongoClient(self.uri, **self.spec) as client:
      return client[self.db].command("collstats", self.coll).get("avgObjSize")

  def split(self, desired_bundle_size, start_position=None, stop_position=None):
    desired_bundle_size_in_mb = desired_bundle_size // 1024 // 1024

    # for desired bundle size, if desired chunk size smaller than 1mb, use
    # MongoDB default split size of 1mb.
    if desired_bundle_size_in_mb < 1:
      desired_bundle_size_in_mb = 1

    is_initial_split = start_position is None and stop_position is None
    start_position, stop_position = self._replace_none_positions(
      start_position, stop_position
    )

    if self.bucket_auto:
      # Use $bucketAuto for bundling
      split_keys = []
      weights = []
      for bucket in self._get_auto_buckets(
          desired_bundle_size_in_mb,
          start_position,
          stop_position,
          is_initial_split,
      ):
        split_keys.append({"_id": bucket["_id"]["max"]})
        weights.append(bucket["count"])
    else:
      # Use splitVector for bundling
      split_keys = self._get_split_keys(
          desired_bundle_size_in_mb, start_position, stop_position)
      weights = itertools.cycle((desired_bundle_size_in_mb, ))

    bundle_start = start_position
    for split_key_id, weight in zip(split_keys, weights):
      if bundle_start >= stop_position:
        break
      bundle_end = min(stop_position, split_key_id["_id"])
      yield iobase.SourceBundle(
          weight=weight,
          source=self,
          start_position=bundle_start,
          stop_position=bundle_end,
      )
      bundle_start = bundle_end
    # add range of last split_key to stop_position
    if bundle_start < stop_position:
      # bucket_auto mode can come here if not split due to single document
      weight = 1 if self.bucket_auto else desired_bundle_size_in_mb
      yield iobase.SourceBundle(
          weight=weight,
          source=self,
          start_position=bundle_start,
          stop_position=stop_position,
      )

  def get_range_tracker(
      self,
      start_position: Union[int, str, ObjectId] = None,
      stop_position: Union[int, str, ObjectId] = None,
  ):
    """Returns a MongoDBRangeTracker for a given position range.

    Framework may invoke ``read()`` method with the MongoDBRangeTracker object
    returned here to read data from the source.

    Args:
      start_position: starting position of the range. If 'None' default start
                      position of the source must be used.
      stop_position:  ending position of the range. If 'None' default stop
                      position of the source must be used.
    Returns:
      a ``MongoDBRangeTracker`` for the given position range.
    """
    start_position, stop_position = self._replace_none_positions(
      start_position, stop_position
    )
    return MongoDBRangeTracker(start_position, stop_position)

  def read(self, range_tracker):
    with MongoClient(self.uri, **self.spec) as client:
      all_filters = self._merge_id_filter(
          range_tracker.start_position(), range_tracker.stop_position())
      docs_cursor = (
          client[self.db][self.coll].find(
              filter=all_filters,
              projection=self.projection).sort([("_id", ASCENDING)]))
      for doc in docs_cursor:
        if not range_tracker.try_claim(doc["_id"]):
          return
        yield doc

  def display_data(self):
    res = super().display_data()
    res["database"] = self.db
    res["collection"] = self.coll
    res["filter"] = json.dumps(self.filter, default=json_util.default)
    res["projection"] = str(self.projection)
    res["bucket_auto"] = self.bucket_auto
    return res

  def _get_split_keys(self, desired_chunk_size_in_mb, start_pos, end_pos):
    # calls mongodb splitVector command to get document ids at split position
    if start_pos >= _ObjectIdHelper.increment_id(end_pos, -1):
      # single document not splittable
      return []
    with MongoClient(self.uri, **self.spec) as client:
      name_space = "%s.%s" % (self.db, self.coll)
      return client[self.db].command(
        "splitVector",
        name_space,
        keyPattern={"_id": 1},  # Ascending index
        min={"_id": start_pos},
        max={"_id": end_pos},
        maxChunkSize=desired_chunk_size_in_mb,
      )["splitKeys"]

  def _get_auto_buckets(
      self,
      desired_chunk_size_in_mb,
      start_pos: Union[int, str, ObjectId],
      end_pos: Union[int, str, ObjectId],
      is_initial_split: bool,
  ) -> list:
    """Use MongoDB `$bucketAuto` aggregation to split collection into bundles
    instead of `splitVector` command, which does not work with MongoDB Atlas.
    """
    if start_pos >= _ObjectIdHelper.increment_id(end_pos, -1):
      # single document not splittable
      return []

    if is_initial_split and not self.filter:
      # total collection size in MB
      size_in_mb = self.estimate_size() / float(1 << 20)
    else:
      # size of documents within start/end id range and possibly filtered
      documents_count = self._count_id_range(start_pos, end_pos)
      avg_document_size = self._estimate_average_document_size()
      size_in_mb = documents_count * avg_document_size / float(1 << 20)

    if size_in_mb == 0:
      # no documents not splittable (maybe a result of filtering)
      return []

    bucket_count = math.ceil(size_in_mb / desired_chunk_size_in_mb)
    with beam.io.mongodbio.MongoClient(self.uri, **self.spec) as client:
      pipeline = [
          {
              # filter by positions and by the custom filter if any
              "$match": self._merge_id_filter(start_pos, end_pos)
          },
          {
              "$bucketAuto": {
                  "groupBy": "$_id", "buckets": bucket_count
              }
          },
      ]
      buckets = list(
          # Use `allowDiskUse` option to avoid aggregation limit of 100 Mb RAM
          client[self.db][self.coll].aggregate(pipeline, allowDiskUse=True))
      if buckets:
        buckets[-1]["_id"]["max"] = end_pos

      return buckets

  def _merge_id_filter(
      self,
      start_position: Union[int, str, ObjectId],
      stop_position: Union[int, str, ObjectId] = None,
  ) -> dict:
    """Merge the default filter (if any) with refined _id field range
    of range_tracker.
    $gte specifies start position (inclusive)
    and $lt specifies the end position (exclusive),
    see more at
    https://docs.mongodb.com/manual/reference/operator/query/gte/ and
    https://docs.mongodb.com/manual/reference/operator/query/lt/
    """
    if stop_position is None:
      id_filter = {"_id": {"$gte": start_position}}
    else:
      id_filter = {"_id": {"$gte": start_position, "$lt": stop_position}}

    if self.filter:
      all_filters = {
          # see more at
          # https://docs.mongodb.com/manual/reference/operator/query/and/
          "$and": [self.filter.copy(), id_filter]
      }
    else:
      all_filters = id_filter

    return all_filters

  def _get_head_document_id(self, sort_order):
    with MongoClient(self.uri, **self.spec) as client:
      cursor = (
          client[self.db][self.coll].find(filter={}, projection=[]).sort([
              ("_id", sort_order)
          ]).limit(1))
      try:
        return cursor[0]["_id"]

      except IndexError:
        raise ValueError("Empty Mongodb collection")

  def _replace_none_positions(self, start_position, stop_position):

    if start_position is None:
      start_position = self._get_head_document_id(ASCENDING)
    if stop_position is None:
      last_doc_id = self._get_head_document_id(DESCENDING)
      # increment last doc id binary value by 1 to make sure the last document
      # is not excluded
      stop_position = _ObjectIdHelper.increment_id(last_doc_id, 1)

    return start_position, stop_position

  def _count_id_range(self, start_position, stop_position):
    """Number of documents between start_position (inclusive)
    and stop_position (exclusive), respecting the custom filter if any.
    """
    with MongoClient(self.uri, **self.spec) as client:
      return client[self.db][self.coll].count_documents(
          filter=self._merge_id_filter(start_position, stop_position))


class MongoDBRangeTracker(OrderedPositionRangeTracker):
  """RangeTracker for tracking MongoDB `_id` of following types:
  - int
  - bytes
  - str
  - bson ObjectId

  For bytes/string keys tracks progress through a lexicographically
  ordered keyspace of strings.
  """
  def position_to_fraction(
      self,
      pos: Union[int, bytes, str, ObjectId] = None,
      start: Union[int, bytes, str, ObjectId] = None,
      end: Union[int, bytes, str, ObjectId] = None,
  ) -> float:
    """Returns the fraction of keys in the range [start, end) that
    are less than the given key.
    """
    # Handle integer `_id`
    if isinstance(pos, int) and isinstance(start, int) and isinstance(end, int):
      return (pos - start) / (end - start)

    # Handle ObjectId `_id`
    if (isinstance(pos, ObjectId) and isinstance(start, ObjectId) and
        isinstance(end, ObjectId)):
      pos = _ObjectIdHelper.id_to_int(pos)
      start = _ObjectIdHelper.id_to_int(start)
      end = _ObjectIdHelper.id_to_int(end)
      return (pos - start) / (end - start)

    if not pos:
      return 0

    if start is None:
      start = b""

    prec = len(start) + 7
    if pos.startswith(start):
      # Higher absolute precision needed for very small values of fixed
      # relative position.
      prec = max(prec, len(pos) - len(pos[len(start):].strip(b"\0")) + 7)
    pos = self._bytestring_to_int(pos, prec)
    start = self._bytestring_to_int(start, prec)
    end = self._bytestring_to_int(end, prec) if end else 1 << (prec * 8)

    return (pos - start) / (end - start)

  def fraction_to_position(
      self,
      fraction: float,
      start: Union[int, bytes, str, ObjectId] = None,
      end: Union[int, bytes, str, ObjectId] = None,
  ) -> Union[int, bytes, str, ObjectId]:
    """Converts a fraction between 0 and 1 to a position between start and end.
    For string keys linearly interpolates a key that is lexicographically
    fraction of the way between start and end.
    """
    if not 0 <= fraction <= 1:
      raise ValueError(f"Invalid fraction: {fraction}! Must be in range [0, 1]")

    # Handle integer `_id`
    if isinstance(start, int) and isinstance(end, int):
      total = end - start
      pos = int(total * fraction + start)
      # make sure split position is larger than start position and smaller than
      # end position.
      if pos <= start:
        pos = start + 1
      if pos >= end:
        pos = end - 1
      return pos

    # Handle ObjectId `_id`
    if isinstance(start, (int, ObjectId)) and isinstance(end, (int, ObjectId)):
      start = _ObjectIdHelper.id_to_int(start)
      end = _ObjectIdHelper.id_to_int(end)
      total = end - start
      pos = int(total * fraction + start)
      # make sure split position is larger than start position and smaller than
      # end position.
      if pos <= start:
        pos = start + 1
      if pos >= end:
        pos = end - 1
      return _ObjectIdHelper.int_to_id(pos)

    if start is None:
      start = b""

    if fraction == 1:
      return end

    if fraction == 0:
      return start

    if not end:
      common_prefix_len = len(start) - len(start.lstrip(b"\xFF"))
    else:
      for ix, (s, e) in enumerate(zip(start, end)):
        if s != e:
          common_prefix_len = ix
          break
      else:
        common_prefix_len = min(len(start), len(end))
    # Convert the relative precision of fraction (~53 bits) to an absolute
    # precision needed to represent values between start and end distinctly.
    prec = common_prefix_len + int(-math.log(fraction, 256)) + 7
    start = self._bytestring_to_int(start, prec)
    end = self._bytestring_to_int(end, prec) if end else 1 << (prec * 8)
    pos = start + int((end - start) * fraction)
    # Could be equal due to rounding.
    # Adjust to ensure we never return the actual start and end
    # unless fraction is exactly 0 or 1.
    if pos == start:
      pos += 1
    elif pos == end:
      pos -= 1
    return self._bytestring_from_int(pos, prec).rstrip(b"\0")

  @staticmethod
  def _bytestring_to_int(s: Union[bytes, str], prec: int) -> int:
    """Returns int(256**prec * f) where f is the fraction
    represented by interpreting '.' + s as a base-256
    floating point number.
    """
    if not s:
      return 0

    if isinstance(s, str):
      s = s.encode()

    if len(s) < prec:
      s += b"\0" * (prec - len(s))
    else:
      s = s[:prec]

    return int(codecs.encode(s, "hex"), 16)

  @staticmethod
  def _bytestring_from_int(i: int, prec: int) -> bytes:
    """Inverse of _bytestring_to_int."""
    h: str = "%x" % i
    return codecs.decode("0" * (2 * prec - len(h)) + h, "hex")


class _ObjectIdHelper:
  """A Utility class to manipulate bson object ids."""
  @classmethod
  def id_to_int(cls, _id: Union[int, ObjectId]) -> int:
    """
    Args:
      _id: ObjectId required for each MongoDB document _id field.

    Returns: Converted integer value of ObjectId's 12 bytes binary value.

    """
    if isinstance(_id, int):
      return _id

    # converts object id binary to integer
    # id object is bytes type with size of 12
    ints = struct.unpack(">III", _id.binary)
    return (ints[0] << 64) + (ints[1] << 32) + ints[2]

  @classmethod
  def int_to_id(cls, number):
    """
    Args:
      number(int): The integer value to be used to convert to ObjectId.

    Returns: The ObjectId that has the 12 bytes binary converted from the
      integer value.

    """
    # converts integer value to object id. Int value should be less than
    # (2 ^ 96) so it can be convert to 12 bytes required by object id.
    if number < 0 or number >= (1 << 96):
      raise ValueError("number value must be within [0, %s)" % (1 << 96))
    ints = [
        (number & 0xFFFFFFFF0000000000000000) >> 64,
        (number & 0x00000000FFFFFFFF00000000) >> 32,
        number & 0x0000000000000000FFFFFFFF,
    ]

    number_bytes = struct.pack(">III", *ints)
    return ObjectId(number_bytes)

  @classmethod
  def increment_id(
      cls,
      object_id: Union[int, str, ObjectId],
      inc: int,
  ) -> Union[int, str, ObjectId]:
    """
    Args:
      object_id: The `_id` to change.
      inc(int): The incremental int value to be added to `_id`.

    Returns:
        `_id` incremented by `inc` value
    """
    # Handle integer `_id`
    if isinstance(object_id, int):
      return object_id + inc

    # Handle string `_id`
    if isinstance(object_id, str):
      object_id = object_id or chr(31)  # handle empty string ('')
      # incrementing the latest symbol of the string
      return object_id[:-1] + chr(ord(object_id[-1:]) + inc)

    # Handle ObjectId `_id`:
    # increment object_id binary value by inc value and return new object id.
    id_number = _ObjectIdHelper.id_to_int(object_id)
    new_number = id_number + inc
    if new_number < 0 or new_number >= (1 << 96):
      raise ValueError(
          "invalid incremental, inc value must be within ["
          "%s, %s)" % (0 - id_number, 1 << 96 - id_number))
    return _ObjectIdHelper.int_to_id(new_number)


@experimental()
class WriteToMongoDB(PTransform):
  """WriteToMongoDB is a ``PTransform`` that writes a ``PCollection`` of
  mongodb document to the configured MongoDB server.

  In order to make the document writes idempotent so that the bundles are
  retry-able without creating duplicates, the PTransform added 2 transformations
  before final write stage:
  a ``GenerateId`` transform and a ``Reshuffle`` transform.::

                  -----------------------------------------------
    Pipeline -->  |GenerateId --> Reshuffle --> WriteToMongoSink|
                  -----------------------------------------------
                                  (WriteToMongoDB)

  The ``GenerateId`` transform adds a random and unique*_id* field to the
  documents if they don't already have one, it uses the same format as MongoDB
  default. The ``Reshuffle`` transform makes sure that no fusion happens between
  ``GenerateId`` and the final write stage transform,so that the set of
  documents and their unique IDs are not regenerated if final write step is
  retried due to a failure. This prevents duplicate writes of the same document
  with different unique IDs.

  """
  def __init__(
      self,
      uri="mongodb://localhost:27017",
      db=None,
      coll=None,
      batch_size=100,
      extra_client_params=None,
  ):
    """

    Args:
      uri (str): The MongoDB connection string following the URI format
      db (str): The MongoDB database name
      coll (str): The MongoDB collection name
      batch_size(int): Number of documents per bulk_write to  MongoDB,
        default to 100
      extra_client_params(dict): Optional `MongoClient
       <https://api.mongodb.com/python/current/api/pymongo/mongo_client.html>`_
       parameters as keyword arguments

    Returns:
      :class:`~apache_beam.transforms.ptransform.PTransform`

    """
    if extra_client_params is None:
      extra_client_params = {}
    if not isinstance(db, str):
      raise ValueError("WriteToMongoDB db param must be specified as a string")
    if not isinstance(coll, str):
      raise ValueError(
          "WriteToMongoDB coll param must be specified as a string")
    self._uri = uri
    self._db = db
    self._coll = coll
    self._batch_size = batch_size
    self._spec = extra_client_params

  def expand(self, pcoll):
    return (
        pcoll
        | beam.ParDo(_GenerateObjectIdFn())
        | Reshuffle()
        | beam.ParDo(
            _WriteMongoFn(
                self._uri, self._db, self._coll, self._batch_size, self._spec)))


class _GenerateObjectIdFn(DoFn):
  def process(self, element, *args, **kwargs):
    # if _id field already exist we keep it as it is, otherwise the ptransform
    # generates a new _id field to achieve idempotent write to mongodb.
    if "_id" not in element:
      # object.ObjectId() generates a unique identifier that follows mongodb
      # default format, if _id is not present in document, mongodb server
      # generates it with this same function upon write. However the
      # uniqueness of generated id may not be guaranteed if the work load are
      # distributed across too many processes. See more on the ObjectId format
      # https://docs.mongodb.com/manual/reference/bson-types/#objectid.
      element["_id"] = objectid.ObjectId()

    yield element


class _WriteMongoFn(DoFn):
  def __init__(
      self, uri=None, db=None, coll=None, batch_size=100, extra_params=None):
    if extra_params is None:
      extra_params = {}
    self.uri = uri
    self.db = db
    self.coll = coll
    self.spec = extra_params
    self.batch_size = batch_size
    self.batch = []

  def finish_bundle(self):
    self._flush()

  def process(self, element, *args, **kwargs):
    self.batch.append(element)
    if len(self.batch) >= self.batch_size:
      self._flush()

  def _flush(self):
    if len(self.batch) == 0:
      return
    with _MongoSink(self.uri, self.db, self.coll, self.spec) as sink:
      sink.write(self.batch)
      self.batch = []

  def display_data(self):
    res = super(_WriteMongoFn, self).display_data()
    res["database"] = self.db
    res["collection"] = self.coll
    res["batch_size"] = self.batch_size
    return res


class _MongoSink:
  def __init__(self, uri=None, db=None, coll=None, extra_params=None):
    if extra_params is None:
      extra_params = {}
    self.uri = uri
    self.db = db
    self.coll = coll
    self.spec = extra_params
    self.client = None

  def write(self, documents):
    if self.client is None:
      self.client = MongoClient(host=self.uri, **self.spec)
    requests = []
    for doc in documents:
      # match document based on _id field, if not found in current collection,
      # insert new one, otherwise overwrite it.
      requests.append(
          ReplaceOne(
              filter={"_id": doc.get("_id", None)},
              replacement=doc,
              upsert=True))
    resp = self.client[self.db][self.coll].bulk_write(requests)
    _LOGGER.debug(
        "BulkWrite to MongoDB result in nModified:%d, nUpserted:%d, "
        "nMatched:%d, Errors:%s" % (
            resp.modified_count,
            resp.upserted_count,
            resp.matched_count,
            resp.bulk_api_result.get("writeErrors"),
        ))

  def __enter__(self):
    if self.client is None:
      self.client = MongoClient(host=self.uri, **self.spec)
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    if self.client is not None:
      self.client.close()
