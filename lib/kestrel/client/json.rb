require 'json'

module Kestrel
  class Client
    class Json < Proxy
      def get(*args)
        response = client.get(*args)
        if response.is_a?(String)
          HashWithIndifferentAccess.new(JSON.parse(response)) rescue response
        else
          response
        end
      end
    end
  end
end
