## kestrel-client: Talk to Kestrel queue server from Ruby

kestrel-client is a library that allows you to talk to a [Kestrel](http://github.com/robey/kestrel) queue server from ruby. As Kestrel uses the memcache protocol, kestrel-client is implemented as a wrapper around the memcached gem.


## Installation

you will need to install memcached.gem, though rubygems should do this for you. just:

    sudo gem install kestrel-client


## Basic Usage

`Kestrel::Client.new` takes a list of servers and an options hash. See the [rdoc for Memcached](http://blog.evanweaver.com/files/doc/fauna/memcached/classes/Memcached.html) for an explanation of what the various options do.

    require 'kestrel'

    $queue = Kestrel::Client.new('localhost:22133')
    $queue.set('a_queue', 'foo')
    $queue.get('a_queue') # => 'foo'


## Client Proxies

kestrel-client comes with a number of decorators that change the behavior of the raw client.

    $queue = Kestrel::Client.new('localhost:22133')
    $queue.get('empty_queue') # => nil

    $queue = Kestrel::Client::Blocking.new(Kestrel::Client.new('localhost:22133'))
    $queue.get('empty_queue') # does not return until it pulls something from the queue


## Configuration Management

Kestrel::Config provides some tools for pulling queue config out of a YAML config file.

    Kestrel::Config.load 'path/to/kestrel.yml'
    Kestrel::Config.environment = 'production' # defaults to development

    $queue = Kestrel::Config.new_client

This tells kestrel-client to look for `path/to/kestrel.yml`, and pull the client configuration out of
the 'production' key in that file. Sample config:

    defaults: &defaults
      distribution: :random
      timeout: 2
      connect_timeout: 1

    production:
      <<: *defaults
      servers:
        - kestrel01.example.com:22133
        - kestrel02.example.com:22133
        - kestrel03.example.com:22133

    development:
      <<: *defaults
      servers:
        - localhost:22133
      show_backtraces: true


## License

Copyright 2010 Twitter, Inc.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

