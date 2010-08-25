module Kestrel
  class Client < Memcached::Rails
    require 'kestrel/client/stats_helper'

    autoload :Proxy, 'kestrel/client/proxy'
    autoload :Envelope, 'kestrel/client/envelope'
    autoload :Blocking, 'kestrel/client/blocking'
    autoload :Partitioning, 'kestrel/client/partitioning'
    autoload :Unmarshal, 'kestrel/client/unmarshal'
    autoload :Namespace, 'kestrel/client/namespace'
    autoload :Json, 'kestrel/client/json'
    autoload :Reliable, "kestrel/client/reliable"
    autoload :Retrying, "kestrel/client/retrying"

    KESTREL_OPTIONS = [:gets_per_server, :no_wait].freeze

    DEFAULT_OPTIONS = {
      :retry_timeout => 0,
      :exception_retry_limit => 0,
      :timeout => 0.25,
      :gets_per_server => 100
    }.freeze

    include StatsHelper

    attr_reader :current_queue, :kestrel_options

    alias get_from_random get

    def initialize(servers = nil, opts = {})
      opts[:distribution] = :random # force random distribution
      opts = DEFAULT_OPTIONS.merge(opts)
      super servers, extract_kestrel_options(opts)
    end

    # ==== Parameters
    # key<String>:: Queue name
    # opts<Boolean,Hash>:: True/false toggles Marshalling. A Hash
    #                      allows collision-avoiding options support.
    #
    # ==== Options (opts)
    # :open<Boolean>:: Begins a reliable read.
    # :close<Boolean>:: Ends a reliable read.
    # :abort<Boolean>:: Cancels an existing reliable read
    # :peek<Boolean>:: Return the head of the queue, without removal
    # :timeout<Integer>:: Milliseconds to block for a new item
    # :raw<Boolean>:: Toggles Marshalling. Equivalent to the "old
    #                 style" second argument.
    #
    def get(key, opts = {})
      raw = opts.delete(:raw) || false
      commands = extract_queue_commands(opts)

      val =
        begin
          send(select_get_method(key), key + commands, raw)
        rescue Memcached::NotFound, Memcached::ATimeoutOccurred, Memcached::ServerIsMarkedDead
          # we can't tell the difference between a server being down
          # and an empty queue, so just return nil. our sticky server
          # logic should eliminate piling on down servers
          nil
        end

      # nil result, force next get to jump from current server
      @counter = @gets_per_server unless val

      val
    end

    def flush(queue)
      count = 0
      while sizeof(queue) > 0
        while get queue, :raw => true
          count += 1
        end
      end
      count
    end

    def peek(queue)
      get queue, :peek => true
    end

    private

    def extract_kestrel_options(opts)
      @kestrel_options, opts = opts.inject([{}, {}]) do |(kestrel, memcache), (key, opt)|
        (KESTREL_OPTIONS.include?(key) ? kestrel : memcache)[key] = opt
        [kestrel, memcache]
      end
      opts
    end

    def select_get_method(key)
      @counter ||= 0
      @gets_per_server ||= kestrel_options[:gets_per_server]

      if key != @current_queue || @counter >= @gets_per_server
        @counter = 0
        @current_queue = key
        :get_from_random
      else
        @counter +=1
        :get_from_last
      end
    end

    def extract_queue_commands(opts)
      commands = [:open, :close, :abort, :peek].select do |key|
        opts[key]
      end

      if timeout = (opts[:timeout] || default_get_timeout)
        commands << "t=#{timeout}"
      end

      commands.map { |c| "/#{c}" }.join('')
    end

    def default_get_timeout
      @default_timeout ||= (options[:timeout] * 1000).to_i unless kestrel_options[:no_wait]
    end
  end
end
