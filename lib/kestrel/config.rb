require 'yaml'

module Kestrel
  module Config
    class ConfigNotLoaded < StandardError; end

    extend self

    attr_accessor :environment, :config

    def load(config_file)
      self.config = YAML.load_file(config_file)
    end

    def environment
      @environment ||= 'development'
    end

    def config
      @config or raise ConfigNotLoaded
    end

    def namespace(namespace)
      client_args_from config[namespace.to_s][environment.to_s]
    end

    def default
      client_args_from config[environment.to_s]
    end

    def new_client(space = nil)
      Client.new *(space ? namespace(space) : default)
    end

    alias method_missing namespace

    private

    def client_args_from(config)
      sanitized = config.inject({}) do |sanitized, (key, val)|
        sanitized[key.to_sym] = val; sanitized
      end
      servers = sanitized.delete(:servers)

      [servers, sanitized]
    end
  end
end
