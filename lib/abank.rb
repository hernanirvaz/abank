# frozen_string_literal: true

require 'thor'
require 'abank/version'
require 'abank/bigquery'
require 'abank/folhacalculo'

module Abank
  ID = `whoami`.chomp

  class Error < StandardError; end

  # CLI para carregar folhas calculo comuns no bigquery
  class CLI < Thor
    class_option :d, banner: 'DIR',
                     default: "/home/#{ID}/Downloads",
                     desc: 'Onde procurar folhas calculo'
    class_option :x, banner: 'EXT',
                     default: '.xlsx',
                     desc: 'Extensao das folhas calculo'

    desc 'load', 'carrega dados xlsx no bigquery'
    # processa xlsx
    def load
      Dir.glob("#{options[:d]}/*#{options[:x]}").sort.each do |f|
        Bigquery.new(f).processa
      end
    end

    desc 'mostra', 'mostra dados do xlsx'
    # mostra xlsx
    def mostra
      Dir.glob("#{options[:d]}/*#{options[:x]}").sort.each do |f|
        Bigquery.new(f).show
      end
    end

    desc 'classifica', 'classifica arquivo no bigquery'
    # classifica bigquery
    def classifica
      Bigquery.new('').sql_update
    end

    default_task :mostra
  end
end
