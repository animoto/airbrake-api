module Hoptoad
  class Project < Hoptoad::Base

    def self.find(*args)
      setup
      options = args.extract_options!

      results = fetch(collection_path, options)

      raise HoptoadError.new(results.errors.error) if results.errors
      results.projects.project
    end

    def self.collection_path
      '/data_api/v1/projects.xml'
    end

  end
end