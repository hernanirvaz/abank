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
    desc 'load', 'carrega dados da folha calculo no bigquery'
    option :d, banner: 'DIR', default: "/home/#{ID}/Downloads",
               desc: 'Onde procurar folhas calculo'
    option :x, banner: 'EXT', default: '.xlsx',
               desc: 'Extensao das folhas calculo'
    option :s, type: :boolean, default: false,
               desc: 'apaga linhas similares no bigquery'
    option :e, type: :boolean, default: false,
               desc: 'apaga linhas existentes no bigquery'
    # processa xlsx
    def load
      # opcoes apagar linhas
      d = options.select { |_, v| [true, false].include?(v) }
      Dir.glob("#{options[:d]}/*#{options[:x]}").sort.each do |f|
        Bigquery.new(f, d).processa
      end
    end

    desc 'mostra', 'mostra dados da folha calculo'
    option :d, banner: 'DIR', default: "/home/#{ID}/Downloads",
               desc: 'Onde procurar folhas calculo'
    option :x, banner: 'EXT', default: '.xlsx',
               desc: 'Extensao das folhas calculo'
    # mostra xlsx
    def mostra
      Dir.glob("#{options[:d]}/*#{options[:x]}").sort.each do |f|
        Bigquery.new(f).show
      end
    end

    desc 'classifica', 'classifica arquivo no bigquery'
    # classifica bigquery
    def classifica
      Bigquery.new.sql_update
    end

    default_task :mostra
  end
end
