require 'faraday_middleware'
require 'parallel'
require 'airbrake-api/core_ext/hash'
require 'airbrake-api/middleware/scrub_response'
require 'airbrake-api/middleware/raise_server_error'
require 'airbrake-api/middleware/raise_response_error'

module AirbrakeAPI
  class Client

    PARALLEL_WORKERS = 10

    attr_accessor *AirbrakeAPI::Configuration::VALID_OPTIONS_KEYS

    def initialize(options={})
      attrs = AirbrakeAPI.options.merge(options)
      AirbrakeAPI::Configuration::VALID_OPTIONS_KEYS.each do |key|
        send("#{key}=", attrs[key])
      end
    end

    def url_for(endpoint, *args)
      path = case endpoint.to_s
      when 'deploys' then deploys_path(*args)
      when 'projects' then '/projects'
      when 'errors' then errors_path(*args)
      when 'error' then error_path(*args)
      when 'notices' then notices_path(*args)
      when 'notice' then notice_path(*args)
      else raise ArgumentError.new("Unrecognized path: #{path}")
      end

      [account_path, path.split('.').first].join('')
    end

    # deploys

    def deploys(project_id, options = {})
      results = request(:get, deploys_path(project_id), options)
      results.projects.respond_to?(:deploy) ? results.projects.deploy : []
    end

    def deploys_path(project_id)
      "/projects/#{project_id}/deploys.xml"
    end

    # projects
    def projects_path
      '/data_api/v1/projects.xml'
    end

    def projects(options = {})
      results = request(:get, projects_path, options)
      results.projects.project
    end

    # errors

    def unformatted_error_path(error_id)
      "/groups/#{error_id}"
    end

    def error_path(error_id)
      "#{unformatted_error_path(error_id)}.xml"
    end

    def errors_path(options={})
      "#{options[:project_id] ? "/projects/#{options[:project_id]}" : nil}/groups.xml"
    end

    def update(error, options = {})
      results = request(:put, unformatted_error_path(error), options)
      results.group
    end

    def error(error_id, options = {})
      results = request(:get, error_path(error_id), options)
      results.group || results.groups
    end

    def errors(options = {})
      options = options.dup
      project_id = options.delete(:project_id)
      results = request(:get, errors_path(:project_id => project_id), options)
      results.group || results.groups
    end

    def errors_since(since, to = Time.now)
      page = 1
      all_errors_in_range = []

      while (batch = errors(:page => page)).size != 0 do
        in_range = batch.select { |e| (since < e.most_recent_notice_at) && (e.most_recent_notice_at <= to) }
        all_errors_in_range += in_range
        if in_range.size != batch.size
          break
        end
        page += 1
      end

      all_errors_in_range
    end

    # notices

    def notice_path(notice_id, error_id)
      "/groups/#{error_id}/notices/#{notice_id}.xml"
    end

    def notices_path(error_id)
      "/groups/#{error_id}/notices.xml"
    end

    def notice(notice_id, error_id, options = {})
      hash = request(:get, notice_path(notice_id, error_id), options)
      hash.notice
    end

    def notices(error_id, options = {}, &block)
      # a specific page is requested, only return that page
      # if no page is specified, start on page 1
      if options[:page]
        page = options[:page]
        options[:pages] = 1
      else
        page = 1
      end

      notices = []
      page_count = 0
      while !options[:pages] || (page_count + 1) <= options[:pages]
        data = request(:get, notices_path(error_id), :page => page + page_count)

        batch = if options[:raw]
          data.notices
        else
          # get info like backtraces by doing another api call to notice
          Parallel.map(data.notices, :in_threads => number_of_parallel_workers) do |notice_stub|
            notice(notice_stub.id, error_id)
          end
        end
        yield batch if block_given?
        batch.each{|n| notices << n }

        break if batch.size < per_page
        page_count += 1
      end
      notices
    end

    def notices_since(error_id, since, to = Time.now)
      page = 1
      all_notices_in_range = []

      while (batch = notices(error_id, :page => page, :raw => true)).size != 0 do
        in_range = batch.select { |n| (since < n.created_at) && (n.created_at <= to) }
        all_notices_in_range += in_range
        if in_range.size != batch.size
          break
        end
        page += 1
      end

      all_notices_in_range
    end

    def all_notices_since(since, to=Time.now)
      projects_by_id = projects.inject({}) { |memo, p| memo[p['id'].to_i] = p; memo }
      errors = errors_since(since, to)

      all_notices = []

      errors.map do |e|
        notices_for_error = notices_since(e.id, since, to)
        notices_for_error.each do |n| 
          n.error_id = e.id
          n.error_project_id = e.project_id
          p = projects_by_id[n.project_id] || projects_by_id[n.error_project_id]
          if p
            n.project_name = p['name']
          end

          n.error_class = e.error_class

        end
        all_notices += notices_for_error
      end

      all_notices
    end

    private

    def per_page
      @per_page || AirbrakeAPI::Configuration::DEFAULT_PAGE_SIZE
    end

    def number_of_parallel_workers
      @parallel_workers || AirbrakeAPI::DEFAULT_PARALLEL_WORKERS
    end

    def account_path
      "#{protocol}://#{@account}.airbrake.io"
    end

    def protocol
      @secure ? "https" : "http"
    end

    # Perform an HTTP request
    def request(method, path, params = {}, options = {})

      raise AirbrakeError.new('API Token cannot be nil') if @auth_token.nil?
      raise AirbrakeError.new('Account cannot be nil') if @account.nil?

      response = connection(options).run_request(method, nil, nil, nil) do |request|
        case method
        when :delete, :get
          request.url(path, params.merge(:auth_token => @auth_token))
        when :post, :put
          request.url(path, :auth_token => @auth_token)
          request.body = params unless params.empty?
        end
      end
      response.body
    end

    def connection(options={})
      default_options = {
        :headers => {
          :accept => 'application/xml',
          :user_agent => user_agent,
        },
        :ssl => {:verify => false},
        :url => account_path,
      }
      @connection ||= Faraday.new(default_options.deep_merge(connection_options)) do |builder|
        builder.use Faraday::Request::UrlEncoded
        builder.use AirbrakeAPI::Middleware::RaiseResponseError
        builder.use FaradayMiddleware::Mashify
        builder.use FaradayMiddleware::ParseXml
        builder.use AirbrakeAPI::Middleware::ScrubResponse
        builder.use AirbrakeAPI::Middleware::RaiseServerError

        builder.adapter adapter
      end
    end

  end
end
