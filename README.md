# Abank [![Build Status](https://travis-ci.com/hernanirvaz/abank.svg?branch=master)](https://travis-ci.com/hernanirvaz/abank)

Arquiva movimentos conta-corrente, conta-cartao do activobank no bigquery. Permite apagar movimentos similares/existentes ja no bigquery. Permite ainda classificar movimentos ja no bigquery.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'abank'
```

And then execute:

    $ bundle install

Or install it yourself as:

    $ gem install abank

## Usage

    $ abank help [COMANDO]  # ajuda aos comandos
    $ abank mostra          # mostra dados da folha calculo
    $ abank load            # carrega dados da folha calculo no bigquery
    $ abank classifica      # classifica arquivo no bigquery

## Development

After checking out the repo, run `bin/setup` to install dependencies. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
