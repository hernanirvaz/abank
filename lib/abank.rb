# frozen_string_literal: true

require 'thor'
require 'abank/bigquery'
require 'abank/folhacalculo'
require 'abank/version'

# @author Hernani Rodrigues Vaz
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
    option :n, banner: 'NUM', type: :numeric, default: 0,
               desc: 'Correcao dias para data valor'
    option :s, type: :boolean, default: false,
               desc: 'apaga linha similar no bigquery'
    option :e, type: :boolean, default: false,
               desc: 'apaga linha igual no bigquery'
    option :m, type: :boolean, default: false,
               desc: 'apaga linhas existencia multipla no bigquery'
    # processa folha calculo
    def load
      Dir.glob("#{options[:d]}/*#{options[:x]}").sort.each do |f|
        Bigquery.new(f, load_ops).processa
      end
    end

    desc 'mostra', 'mostra dados da folha calculo'
    option :d, banner: 'DIR', default: "/home/#{ID}/Downloads",
               desc: 'Onde procurar folhas calculo'
    option :x, banner: 'EXT', default: '.xlsx',
               desc: 'Extensao das folhas calculo'
    # mostra folha calculo
    def mostra
      Dir.glob("#{options[:d]}/*#{options[:x]}").sort.each do |f|
        Bigquery.new(f).processa
      end
    end

    desc 'classifica', 'classifica arquivo no bigquery'
    # classifica arquivo no bigquery
    def classifica
      Bigquery.new.classifica
    end

    no_commands do
      # @return [Hash] ops opcoes trabalho com linhas para load
      def load_ops
        { s: options[:s], e: options[:e],
          m: options[:m], i: true, n: options[:n] }
      end
    end

    default_task :mostra
  end
end
