require 'airbrake-api/version'

module AirbrakeAPI
  module Configuration
    VALID_OPTIONS_KEYS = [
      :account,
      :auth_token,
      :secure,
      :connection_options,
      :adapter,
      :user_agent,
      :per_page,
      :parallel_workers
    ]

    attr_accessor *VALID_OPTIONS_KEYS

    DEFAULT_ADAPTER     = :net_http
    DEFAULT_USER_AGENT  = "AirbrakeAPI Ruby Gem #{AirbrakeAPI::VERSION}"
    DEFAULT_CONNECTION_OPTIONS = {}
    DEFAULT_PAGE_SIZE = 20
    DEFAULT_PARALLEL_WORKERS = 10

    def self.extended(base)
      base.reset
    end

    def configure(options={})
      @account    = options[:account] if options.has_key?(:account)
      @auth_token = options[:auth_token] if options.has_key?(:auth_token)
      @secure     = options[:secure] if options.has_key?(:secure)
      yield self if block_given?
      self
    end

    def options
      options = {}
      VALID_OPTIONS_KEYS.each{|k| options[k] = send(k)}
      options
    end

    def account_path
      "#{protocol}://#{@account}.airbrake.io"
    end

    def protocol
      @secure ? "https" : "http"
    end

    def reset
      @account    = nil
      @auth_token = nil
      @secure     = false
      @adapter    = DEFAULT_ADAPTER
      @user_agent = DEFAULT_USER_AGENT
      @connection_options = DEFAULT_CONNECTION_OPTIONS
      @per_path = DEFAULT_PAGE_SIZE
      @parallel_workers = DEFAULT_PARALLEL_WORKERS
    end

  end
end
