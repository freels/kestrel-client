module Kestrel
  class Client < Memcached::Rails
    autoload :Proxy, 'kestrel/client/proxy'
    autoload :Envelope, 'kestrel/client/envelope'
    autoload :Blocking, 'kestrel/client/blocking'
    autoload :Partitioning, 'kestrel/client/partitioning'
    autoload :Unmarshal, 'kestrel/client/unmarshal'
    autoload :Namespace, 'kestrel/client/namespace'
    autoload :Json, 'kestrel/client/json'


    QUEUE_STAT_NAMES = %w{items bytes total_items logsize expired_items mem_items mem_bytes age discarded}

    def flush(queue)
      count = 0
      while sizeof(queue) > 0
        while get(queue, true)
          count += 1
        end
      end
      count
    end

    def peek(queue)
      val = get(queue)
      set(queue, val)
      val
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
