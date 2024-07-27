#!/usr/bin/python3
#
# asyncpg_utility.py
#

#
# MIT License
# 
# Copyright (c) 2019 Bitmodeler
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import sys
import errno
import re

import asyncio
import asyncpg

from unittest import TestCase

# convenience functions...

def run_until_complete(promise):
    return asyncio.get_event_loop().run_until_complete(promise)

async def allocate_pool(host, database, user, pw):
          return await asyncpg.create_pool(host=host,
                                           database=database,
                                           user=user,
                                           password=pw,
                                           statement_cache_size=0 # MagicStack bug workaround...
                                          )

# constructor defaults...
case_sensitive=False
parameter_markers=['{{', '}}']
timeout=None
column=0
close_event_loop_on_err=True
exit_on_err=True

class NamedParameterQuery:
      # extension to asyncpg that supports named parameters in queries
      #
      # Code example:
      #
      #      import asyncio
      #      import asyncpg
      #      from asyncpg_utility import NamedParameterQuery, NamedParameterConnection
      #      # .
      #      # .
      #      # .
      #      query=query='SELECT * FROM my_table WHERE name={{NAME}} AND age={{AGE}}'
      #      my_named_parameter_query=NamedParameterQuery(query)
      #      # .
      #      # .
      #      # .
      #      async def my_async_routine():
      #                # .
      #                # .
      #                # .
      #                # "conn" is your vanilla asyncpg database connection
      #                # (obtained from "await asyncpg.connect(...", or acquired from a pool...)
      #                my_named_parameter_conn=NamedParameterConnection(conn, my_named_parameter_query)
      #
      #                # Note how the order of the arguments
      #                # to NamedParameterConnection's fetchrow
      #                # doesn't matter now...
      #                await result=my_named_parameter_conn.fetchrow(age=20, name='Mr.Big')
      #
      # OR: my_named_parameter_conn.fetch(...
      # OR: my_named_parameter_conn.fetchval(...
      # OR: my_named_parameter_conn.execute(...
      #
      # <grin>Of course, you could go with more meaningful names than the "my_..."
      # ones used here...</grin>
      #
      # If you forget _named_ parameters, an exception will be raised
      # (which is a Good Thing -the exception, not your having made a mistake...)
      # The NamedParameterQuery constructor accepts a "case_sensitive" argument
      # (default: False); so by setting case_sensitive to True, you could _require_
      # that the named parameter in the initial query value in the example above be
      # "{{age}}"; or, correspondingly, the parameter to the fetch routine
      # be "AGE=20".
      #
      # You can choose other parameter markers on the NamedParameterQuery constructor
      # besides the default of "{{", "}}" by setting the "parameter_markers" constructor
      # argument to a two-component list.
      # [example: NamedParameterQuery(..., parameter_markers=['<<', '>>'])]
      # To avoid confusion, it is recommended that you not choose markers that would
      # be likely to collide with SQL syntax, or are used in literal strings
      # by your queries.
      #
      # The "timeout" and "column" parameters to the NamedParameterConnection class's
      # execute and fetch routines are class members initialized by the class's
      # constructor (since ALL arguments to execute and fetch routines are now treated
      # as named parameters to an embedded SQL query); so if you should need those
      # adjusted from one fetch to the next, they can be set before the fetch.
      # [example: my_named_parameter_connection.timeout=2000]
      #
      # The following arguments can be set at module level so that constructor defaults
      # change accordingly for all subsequent class instantiations:
      #
      #     case_sensitive (True/False, default is False)
      #     parameter_markers (default is ['{{', '}}'])
      #     timeout (for fetches, default is 0)
      #     column (for fetchval's, default is 0)
      #     close_on_err (True/False, default is True)
      #     exit_on_err (True/False, default is True)
      #
      # So by doing (just after import of facilities from asyncpg_utility...):
      #
      #              asyncpg_utility.parameter_markers=['{', '}']
      #
      # ALL subsequent NamedParameterQuery's will use that as their default.
      #
      # Strategy: you want to ideally initialize a NamedParameterQuery variable _once_
      # in your program; then once a connection is established (or acquired from a pool),
      # initialize a NamedParameterConnection before using its execute or fetch
      # member functions.

      _WHITESPACE=r'\s*'

      def _ab_end (self, exit_code=None):
          if self._close_event_loop_on_err:
             asyncio.get_event_loop().close ()

          if self._exit_on_err:
             sys.exit (exit_code)

      @staticmethod
      def _before(text, subtext):
          offset=text.find(subtext)
          if offset>=0:
             return text[:offset]

          return None

      @staticmethod
      def _after(text, subtext):
          offset=text.find(subtext)
          if offset>=0:
             return text[offset+len(subtext):]

          return None

      def _mismatched_markers (self, keyword_query, parameter_markers):
          raise ValueError('mismatched parameter markers '+
                           '("'+parameter_markers[0]+'", '+
                            '"'+parameter_markers[1]+'"):\n'+
                           keyword_query
                          )
          self._ab_end (errno.EINVAL) # Invalid argument...

      def __init__(self,
                   keyword_query,
                   case_sensitive=case_sensitive,

                   # This version does not support escaping of parameter markers
                   # (in other words: they are reserved!)
                   # The parameter markers _should not_ be tokens
                   # typically used in SQL queries.
                   parameter_markers=parameter_markers,

                   # Error handling (some people might want a ready-to-use
                   # fail-safe option...)
                   close_event_loop_on_err=close_event_loop_on_err,
                   exit_on_err=exit_on_err
                  ):
          self._keyword_query=keyword_query
          self._case_sensitive=case_sensitive
          self._parameter_markers=parameter_markers
          self._query=keyword_query
          self._close_event_loop_on_err=close_event_loop_on_err
          self._exit_on_err=exit_on_err
          if parameter_markers[0] in keyword_query:
             if parameter_markers[1] in self._before(keyword_query,
                                                     parameter_markers[0]
                                                    ):
                self._mismatched_markers (keyword_query, parameter_markers)
                return # __init__

             self._parameters=self._after(keyword_query, parameter_markers[0])
             last_parameter_marker=self._parameters.rfind(parameter_markers[1])
             if last_parameter_marker<0:
                self._mismatched_markers (keyword_query, parameter_markers)
                return # __init__

             if parameter_markers[0] in self._parameters[last_parameter_marker+
                                                         len(parameter_markers[1]):
                                                        ]:
                self._mismatched_markers (keyword_query, parameter_markers)
                return # __init__

             self._parameters=self._parameters[:last_parameter_marker]
             self._parameters=self._parameters.strip()
             parameter_delimiter=re.compile(self._WHITESPACE+
                                            re.escape(parameter_markers[1])+
                                            '.*?'+ # non-greedy!
                                            re.escape(parameter_markers[0])+
                                            self._WHITESPACE
                                           )

             # Insure regex execution by getting rid of line boundaries
             # (SQL does not need them...);
             # another option would be to emit a warning
             # when newlines are encountered in a prepared query.
             # If you need to use newlines in a query with this class,
             # they should be escaped.
             self._parameters=self._parameters.replace('\r\n', ' ') # Windows?
             self._parameters=self._parameters.replace('\r', ' ')
             self._parameters=self._parameters.replace('\n', ' ')

             self._parameters=re.split(parameter_delimiter, self._parameters)
             if not case_sensitive:
                self._parameters=list(map(str.upper, self._parameters))

             self._parameters=list(dict.fromkeys(self._parameters)) # remove duplicates...

             for index in range(len(self._parameters)):
                 if parameter_markers[0] in self._parameters[index]:
                    self._mismatched_markers (keyword_query, parameter_markers)
                    return # __init__

                 parameter=parameter_markers[0]+self._WHITESPACE+ \
                           self._parameters[index]+ \
                           self._WHITESPACE+parameter_markers[1]

                 if case_sensitive:
                    flags=None

                 else:
                       flags=re.IGNORECASE

                 self._query=re.sub(parameter,
                                    '$'+str(index+1),
                                    self._query, flags=flags
                                   )

          else: # not (parameter_markers[0] in keyword_query)
                self._parameters=[]

          if parameter_markers[1] in self._query:
             self._mismatched_markers (keyword_query, parameter_markers)
             return # __init__

          return # __init__

      @property
      def keyword_query(self):
          return self._keyword_query

      @property
      def case_sensitive(self):
          return self._case_sensitive

      @property
      def parameter_markers(self):
          return self._parameter_markers

      @property
      def parameters(self):
          return self._parameters

      @property
      def query(self): # query using $1, $2, ... placeholders
          return self._query

class NamedParameterConnection:
      def _ab_end (self, exit_code=None):
          if self._close_event_loop_on_err:
             asyncio.get_event_loop().close ()

          if self._exit_on_err:
             sys.exit (exit_code)

      def _bad_arg (self, arg_name, expected_type):
          raise ValueError('Bad argument for "'+arg_name+'"; expected '+expected_type)
          _ab_end (errno.EINVAL) # Invalid argument...

      def __init__(self,
                   connection=None, named_parameter_query=None,

                   # pass-throughs for fetch arguments
                   # (if they need to be changed from one fetch to the next,
                   #  modify the class members in the named parameter connection
                   #  class, before the fetch...)
                   timeout=timeout, column=column,

                   close_event_loop_on_err=close_event_loop_on_err,
                   exit_on_err=exit_on_err
                  ):
          self._close_event_loop_on_err=close_event_loop_on_err
          self._exit_on_err=exit_on_err

          if not isinstance(connection, asyncpg.connection.Connection):
             self._bad_arg ('connection', 'asyncpg.connection.Connection')
             return

          if not isinstance(named_parameter_query, NamedParameterQuery):
             self._bad_arg ('named_parameter_query', 'NamedParameterQuery')
             return

          self._connection=connection
          self._named_parameter_query=named_parameter_query

          # fetch settings...
          self.timeout=timeout
          self.column=column

      def _key_err (self, message):
          raise KeyError(message)
          self._ab_end (errno.EINVAL) # Invalid argument...

      @staticmethod
      def _plural(count, suffix='s'):
          if count==1:
             return ''

          return suffix

      def _values(self, parameters):
          if self._named_parameter_query._case_sensitive:
             fetch_parameters=parameters
             leftover_parameters=set(parameters.keys())

          else:
                fetch_parameters={}
                for parameter in parameters:
                    fetch_parameters[parameter.upper()]=parameters[parameter]

                leftover_parameters=set(map(str.upper, parameters.keys()))

          values=[]
          missing_parameters=[]
          for parameter in self._named_parameter_query._parameters:
              case_sensitive_parameter=parameter
              if not self._named_parameter_query._case_sensitive:
                 parameter=parameter.upper()

              if not (parameter in fetch_parameters):
                 missing_parameters+=[case_sensitive_parameter]
                 values+=[None]

              else:
                    values+=[fetch_parameters[parameter]]

              leftover_parameters.remove (parameter)

          if len(missing_parameters)>0:
             self._key_err ('Query missing the following parameter'+self._plural(len(missing_parameters))+': '+
                            ', '.join(missing_parameters)
                           )
             return None

          if len(leftover_parameters)>0:
             self._key_err ('Unreferenced parameter'+self._plural(len(leftover_parameters))+' '+
                            'passed to query: '+', '.join(leftover_parameters)
                           )
             return None

          return values

      async def execute(self, **parameters):
                return await self._connection.execute(self._named_parameter_query._query,
                                                      *self._values(parameters),
                                                      timeout=self.timeout
                                                     )

      async def fetch(self, **parameters):
                return await self._connection.fetch(self._named_parameter_query._query,
                                                    *self._values(parameters),
                                                    timeout=self.timeout
                                                   )

      async def fetchrow(self, **parameters):
                return await self._connection.fetchrow(self._named_parameter_query._query,
                                                       *self._values(parameters),
                                                       timeout=self.timeout
                                                      )

      async def fetchval(self, **parameters):
                return await self._connection.fetchval(self._named_parameter_query._query,
                                                       *self._values(parameters),
                                                       column=self.column,
                                                       timeout=self.timeout
                                                      )
 
class NamedParameterQueryTest(TestCase):
      # The unit testing primarily tests expected types for return values
      # using a query and connection of the user's choosing...
      KEYWORD_QUERY='SELECT {{NAME}}::text, {{AGE}}::int FROM table'
      QUERY='SELECT $1::text, $2::int FROM table'
      PARAMETERS=['NAME', 'AGE']

      def test(self):
          named_parameter_query=NamedParameterQuery(self.KEYWORD_QUERY)
          self.assertEqual (named_parameter_query.query, self.QUERY)
          self.assertEqual (named_parameter_query.parameters, self.PARAMETERS)

class NamedParameterConnectionTest(TestCase):
      async def setUp(self,
                      connection=None, named_parameter_query=None,
                      timeout=None, column=0,
                      close_event_loop_on_err=True, exit_on_err=True
                     ):
                self._named_parameter_connection=await NamedParameterConnection(connection,
                                                                                named_parameter_query
                                                                               )

      async def test_fetch(self, **parameters):
                result=await self._named_parameter_connection.fetch (parameters)
                self.assertTrue (isinstance(result, list))
                for record in result:
                    self.assertTrue (isinstance(record, Record))

                return result

      async def test_fetchrow(self, **parameters):
                result=await self._named_parameter_connection.fetchrow (parameters)
                self.assertTrue (isinstance(result, Record))
                return result

      async def test_fetchval(self, **parameters):
                result=await self._named_parameter_connection.fetchval (parameters)
                return result

      async def test_execute(self, **parameters):
                result=await self._named_parameter_connection.execute (parameters)
                return result
