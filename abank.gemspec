# frozen_string_literal: true

require_relative 'lib/abank/version'

Gem::Specification.new do |spec|
  spec.name          = 'abank'
  spec.version       = Abank::VERSION
  spec.authors     = ['Hernâni Rodrigues Vaz']
  spec.email       = ['hernanirvaz@gmail.com']
  spec.homepage    = 'https://github.com/hernanirvaz/abank'
  spec.license     = 'MIT'

  spec.summary     = 'Arquiva <conta-corrente>.xlsx,' \
                     ' <conta-cartao>.xlsx no bigquery.'
  spec.description = spec.summary +
                     ' Pode apagar movimentos similares/existentes' \
                     ' ja no bigquery.' \
                     ' Pode ainda classificar movimentos ja no bigquery.'

  spec.metadata['homepage_uri'] = spec.homepage
  spec.metadata['yard.run'] = 'yard'

  spec.required_ruby_version = Gem::Requirement.new('>= 2.3.0')

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads files in RubyGem that have been added into git.
  spec.files = Dir.chdir(File.expand_path(__dir__)) do
    `git ls-files -z`.split("\x0")
                     .reject { |f| f.match(%r{^(test|spec|features)/}) }
  end
  spec.bindir        = 'exe'
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ['lib']

  spec.add_development_dependency 'bundler'
  spec.add_development_dependency 'rake'

  spec.add_dependency 'google-cloud-bigquery'
  spec.add_dependency 'roo'
  spec.add_dependency 'thor'
  spec.add_dependency 'yard'
end
