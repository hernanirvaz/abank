# frozen_string_literal: true

require 'thor'
require 'abank/big'
require 'abank/contrato'
require 'abank/rendas'
require 'abank/folha'
require 'abank/version'

# @author Hernani Rodrigues Vaz
module Abank
  DR = "/home/#{`whoami`.chomp}/Downloads"

  class Error < StandardError; end

  # CLI para carregar folhas calculo comuns no bigquery
  class CLI < Thor
    desc 'tag', 'classifica movimentos'
    # classifica movimentos
    def tag
      Big.new.mv_classifica
    end

    desc 'apagamv', 'apaga movimentos'
    option :k, banner: 'KEY[,KEY...]', required: true, desc: 'keys movimentos a apagar'
    # apaga movimentos
    def apagamv
      Big.new(k: options[:k]).mv_delete.mv_insert.re_work
    end

    desc 'criact', 'cria contrato arrendamento'
    option :c, banner: 'CONTRATO', required: true, desc: 'Identificador contrato arrendamento'
    option :t, type: :boolean, default: true,      desc: 'cria todas as rendas?'
    option :d, banner: 'DATA', default: '',        desc: 'data contrato arrendamento'
    # cria contrato arrendamento
    def criact
      Big.new(c: options[:c], t: options[:t], d: options[:d]).ct_cria
    end

    desc 'apagact', 'apaga contrato arrendamento'
    option :c, banner: 'CONTRATO', required: true, desc: 'Identificador contrato arrendamento'
    option :t, type: :boolean, default: false,     desc: 'apaga todas as rendas?'
    # apaga contrato arrendamento
    def apagact
      Big.new(c: options[:c], t: options[:t]).ct_apaga
    end

    desc 'recriact', 'atualiza rendas de contrato arrendamento'
    option :c, banner: 'CONTRATO', required: true, desc: 'Identificador contrato arrendamento'
    option :t, type: :boolean, default: false,     desc: 'apaga todas as rendas?'
    option :d, banner: 'DATA', default: '',        desc: 'data contrato arrendamento'
    # atualiza rendas de contrato arrendamento
    def recriact
      Big.new(c: options[:c], t: options[:t]).ct_apaga
      Big.new(c: options[:c], t: true, d: options[:d]).ct_cria
    end

    desc 'recriare', 'atualiza rendas dos contratos ativos'
    option :t, type: :boolean, default: false, desc: 'atualiza todas as rendas?'
    # atualiza rendas dos contratos ativos
    def recriare
      Big.new(t: options[:t]).re_atualiza
    end

    desc 'load', 'carrega dados da folha calculo'
    option :s, type: :boolean, default: false, desc: 'apaga movimento similar'
    option :e, type: :boolean, default: false, desc: 'apaga movimento igual'
    option :v, banner: 'DATA', default: '',    desc: 'data valor para movimentos a carregar'
    option :g, banner: 'TAG', default: '',     desc: 'classificacao para movimentos a carregar'
    # carrega folha calculo
    def load
      Dir.glob("#{DR}/*.xlsx").sort.each do |f|
        Folha.new(load_opc.merge(f: f)).processa_xls
      end
    end

    desc 'show', 'mostra dados da folha calculo'
    # mostra folha calculo
    def show
      Dir.glob("#{DR}/*.xlsx").sort.each do |f|
        Folha.new(f: f).processa_xls
      end
    end

    no_commands do
      # @return [Hash] opcoes trabalho com movimentos para load
      def load_opc
        { s: options[:s], e: options[:e], i: true, v: options[:v], g: options[:g] }
      end
    end

    default_task :show
  end
end
