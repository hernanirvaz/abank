# frozen_string_literal: true

require 'thor'
require 'abank/big'
require 'abank/contrato'
require 'abank/folha'
require 'abank/rendas'
require 'abank/version'

# @author Hernani Rodrigues Vaz
module Abank
  ID = `whoami`.chomp

  class Error < StandardError; end

  # CLI para carregar folhas calculo comuns no bigquery
  class CLI < Thor
    desc 'tag', 'classifica movimentos no bigquery'
    # classifica movimentos no bigquery
    def tag
      Big.new(i: true).mv_classifica
    end

    desc 'rendas', 'atualiza rendas no bigquery'
    # atualiza rendas no bigquery
    def rendas
      Big.new.re_atualiza
    end

    desc 'apaga', 'apaga movimentos no bigquery'
    option :k, banner: 'k1[,k2,...]', required: true,
               desc: 'Keys movimentos a apagar'
    # apaga movimentos no bigquery
    def apaga
      Big.new(k: options[:k]).mv_apaga
    end

    desc 'load', 'carrega dados da folha calculo no bigquery'
    option :d, banner: 'DIR', default: "/home/#{ID}/Downloads",
               desc: 'Onde procurar folhas calculo'
    option :v, banner: 'DATA', default: '',
               desc: 'data valor para movimentos a carregar'
    option :g, banner: 'TAG', default: '',
               desc: 'classificacao para movimentos a carregar'
    option :s, type: :boolean, default: false,
               desc: 'apaga linha similar no bigquery'
    option :e, type: :boolean, default: false,
               desc: 'apaga linha igual no bigquery'
    option :m, type: :boolean, default: false,
               desc: 'apaga linhas existencia multipla no bigquery'
    # carrega folha calculo
    def load
      Dir.glob("#{options[:d]}/*.xlsx").sort.each do |f|
        Big::Folha.new(f, load_ops).processa_folha
      end
    end

    desc 'show', 'mostra dados da folha calculo'
    option :d, banner: 'DIR', default: "/home/#{ID}/Downloads",
               desc: 'Onde procurar folhas calculo'
    # mostra folha calculo
    def show
      Dir.glob("#{options[:d]}/*.xlsx").sort.each do |f|
        Big::Folha.new(f).processa_folha
      end
    end

    desc 'criare', 'cria contrato arrendamento/rendas no bigquery'
    option :c, banner: 'CONTRATO', required: true,
               desc: 'Identificador contrato arrendamento a criar'
    option :t, type: :boolean, default: true,
               desc: 'cria todas as rendas?'
    option :v, banner: 'DATA', default: '',
               desc: 'data contrato arrendamento a criar'
    # cria contrato arrendamento/rendas no bigquery
    def criare
      Big::Contrato.new(options[:c], { t: options[:t], v: options[:v] }).re_cria
    end

    desc 'apagare', 'apaga contrato arrendamento/rendas no bigquery'
    option :c, banner: 'CONTRATO', required: true,
               desc: 'Identificador contrato arrendamento a apagar'
    option :t, type: :boolean, default: false,
               desc: 'apaga todas as rendas?'
    # apaga contrato arrendamento/rendas no bigquery
    def apagare
      Big::Contrato.new(options[:c], { t: options[:t], v: '' }).re_apaga
    end

    no_commands do
      # @return [Hash] opcoes trabalho com linhas para load
      def load_ops
        { s: options[:s], e: options[:e], m: options[:m], i: true,
          v: options[:v], g: options[:g] }
      end
    end

    default_task :rendas
  end
end
