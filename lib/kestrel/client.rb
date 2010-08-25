module Kestrel
  class Client < Memcached
    require 'kestrel/client/stats_helper'
    require 'kestrel/client/retry_helper'

    autoload :Proxy, 'kestrel/client/proxy'
    autoload :Envelope, 'kestrel/client/envelope'
    autoload :Blocking, 'kestrel/client/blocking'
    autoload :Partitioning, 'kestrel/client/partitioning'
    autoload :Unmarshal, 'kestrel/client/unmarshal'
    autoload :Namespace, 'kestrel/client/namespace'
    autoload :Json, 'kestrel/client/json'
    autoload :Reliable, "kestrel/client/reliable"

    KESTREL_OPTIONS = [:gets_per_server, :no_wait, :exception_retry_limit].freeze

    DEFAULT_OPTIONS = {
      :retry_timeout => 0,
      :exception_retry_limit => 5,
      :timeout => 0.25,
      :gets_per_server => 100
    }.freeze

    include StatsHelper
    include RetryHelper


    def initialize(*servers)
      opts = servers.last.is_a?(Hash) ? servers.pop : {}
      opts = DEFAULT_OPTIONS.merge(opts)

      @kestrel_options = extract_kestrel_options!(opts)
      @default_get_timeout = (opts[:timeout] / 2 * 1000).to_i unless kestrel_options[:no_wait]
      @gets_per_server = kestrel_options[:gets_per_server]
      @exception_retry_limit = kestrel_options[:exception_retry_limit]
      @counter = 0

      # we handle our own retries so that we can apply different
      # policies to sets and gets, so set memcached limit to 0
      opts[:exception_retry_limit] = 0
      opts[:distribution] = :random # force random distribution

      super Array(servers).flatten.compact, opts
    end


    attr_reader :current_queue, :kestrel_options


    # Memcached overrides

    %w(add append cas decr incr get_orig prepend).each do |m|
      undef_method m
    end

    alias _super_get_from_random get
    private :_super_get_from_random

    def get_from_random(key, raw=false)
      _super_get_from_random key, !raw
    rescue Memcached::NotFound
    end

    def get_from_last(key, raw=false)
      super key, !raw
    rescue Memcached::NotFound
    end

    def delete(key, expiry=0)
      with_retries { super key }
    rescue Memcached::NotFound, Memcached::ServerEnd
    end

    def set(key, value, ttl=0, raw=false)
      with_retries { super key, value, ttl, !raw }
      true
    rescue Memcached::NotStored
      false
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
        rescue Memcached::ATimeoutOccurred, Memcached::ServerIsMarkedDead
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
        count += 1 while get queue, :raw => true
      end
      count
    end

    def peek(queue)
      get queue, :peek => true
    end

    private

    def extract_kestrel_options!(opts)
      kestrel_opts, memcache_opts = opts.inject([{}, {}]) do |(kestrel, memcache), (key, opt)|
        (KESTREL_OPTIONS.include?(key) ? kestrel : memcache)[key] = opt
        [kestrel, memcache]
      end
      opts.replace(memcache_opts)
      kestrel_opts
    end

    def select_get_method(key)
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

      if timeout = (opts[:timeout] || @default_get_timeout)
        commands << "t=#{timeout}"
      end

      commands.map { |c| "/#{c}" }.join('')
    end
  end
end
