module Kestrel
  class Client < Memcached::Rails
    autoload :Proxy, 'kestrel/client/proxy'
    autoload :Envelope, 'kestrel/client/envelope'
    autoload :Blocking, 'kestrel/client/blocking'
    autoload :Partitioning, 'kestrel/client/partitioning'
    autoload :Unmarshal, 'kestrel/client/unmarshal'
    autoload :Namespace, 'kestrel/client/namespace'
    autoload :Json, 'kestrel/client/json'
    autoload :Reliable, "kestrel/client/reliable"
    autoload :Retrying, "kestrel/client/retrying"

    QUEUE_STAT_NAMES = %w{items bytes total_items logsize expired_items mem_items mem_bytes age discarded}

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
    def get(key, opts = false)
      opts     = extract_options(opts)
      raw      = opts.delete(:raw)
      commands = extract_queue_commands(opts)

      super key + commands, raw
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
    def get_from_last(key, opts = {})
      opts     = extract_options(opts)
      raw      = opts.delete(:raw)
      commands = extract_queue_commands(opts)

      super key + commands, raw
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

    def sizeof(queue)
      stat_info = stat(queue)
      stat_info ? stat_info['items'] : 0
    end

    def available_queues
      stats['queues'].keys.sort
    end

    def stats
      merge_stats(servers.map { |server| stats_for_server(server) })
    end

    def stat(queue)
      stats['queues'][queue]
    end

    private

    def extract_options(opts)
      opts.is_a?(Hash) ? opts : { :raw => !!opts }
    end

    def extract_queue_commands(opts)
      commands = [:open, :close, :abort, :peek].select do |key|
        opts[key]
      end

      commands << "t=#{opts[:timeout]}" if opts[:timeout]

      commands.map { |c| "/#{c}" }.join('')
    end

    def stats_for_server(server)
      server_name, port = server.split(/:/)
      socket = TCPSocket.new(server_name, port)
      socket.puts "STATS"

      stats = Hash.new
      stats['queues'] = Hash.new
      while line = socket.readline
        if line =~ /^STAT queue_(\S+?)_(#{QUEUE_STAT_NAMES.join("|")}) (\S+)/
          queue_name, queue_stat_name, queue_stat_value = $1, $2, deserialize_stat_value($3)
          stats['queues'][queue_name] ||= Hash.new
          stats['queues'][queue_name][queue_stat_name] = queue_stat_value
        elsif line =~ /^STAT (\w+) (\S+)/
          stat_name, stat_value = $1, deserialize_stat_value($2)
          stats[stat_name] = stat_value
        elsif line =~ /^END/
          socket.close
          break
        elsif defined?(RAILS_DEFAULT_LOGGER)
          RAILS_DEFAULT_LOGGER.debug("KestrelClient#stats_for_server: Ignoring #{line}")
        end
      end

      stats
    end

    def merge_stats(all_stats)
      result = Hash.new

      all_stats.each do |stats|
        stats.each do |stat_name, stat_value|
          if result.has_key?(stat_name)
            if stat_value.kind_of?(Hash)
              result[stat_name] = merge_stats([result[stat_name], stat_value])
            else
              result[stat_name] += stat_value
            end
          else
            result[stat_name] = stat_value
          end
        end
      end

      result
    end

    def deserialize_stat_value(value)
      case value
        when /^\d+\.\d+$/:
          value.to_f
        when /^\d+$/:
          value.to_i
        else
          value
      end
    end
  end
end
