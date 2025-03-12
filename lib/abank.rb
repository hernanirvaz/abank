# frozen_string_literal: true

require('thor')
require('abank/big')
require('abank/folha')
require('abank/version')

# @author Hernani Rodrigues Vaz
module Abank
  DR = "/home/#{`whoami`.chomp}/Downloads".freeze
  BD = 'hernanilr.ab'

  # CLI para carregar folhas calculo comuns no bigquery
  class CLI < Thor
    desc 'tag', 'classifica movimentos'
    # classifica movimentos
    def tag
      Big.new(options.to_h).mv_classifica.ct_dados.re_insert
    end

    desc 'apagamv', 'apaga movimentos'
    option :k, banner: 'KEY[,KEY...]', required: true, desc: 'keys movimentos a apagar'
    # apaga movimentos
    def apagamv
      Big.new(options.transform_keys(&:to_sym)).mv_delete.ct_dados.re_insert
    end

    desc 'apagact', 'apaga contrato arrendamento'
    option :c, banner: 'CONTRATO', required: true, desc: 'Identificador contrato arrendamento'
    option :t, type: :boolean, default: false,     desc: 'apaga todas as rendas?'
    # apaga contrato arrendamento
    def apagact
      Big.new(options.transform_keys(&:to_sym)).ct_apaga
    end

    desc 'criact', 'cria contrato arrendamento'
    option :c, banner: 'CONTRATO', required: true, desc: 'Identificador contrato arrendamento'
    option :d, banner: 'DATA', default: '',        desc: 'data contrato arrendamento'
    option :t, type: :boolean, default: true,      desc: 'cria todas as rendas?'
    # cria contrato arrendamento
    def criact
      Big.new(options.transform_keys(&:to_sym)).ct_cria
    end

    desc 'recriact', 'atualiza rendas de contrato arrendamento'
    option :c, banner: 'CONTRATO', required: true, desc: 'Identificador contrato arrendamento'
    # atualiza rendas de contrato arrendamento
    def recriact
      opc = options[:c]
      Big.new(c: opc, t: false).ct_apaga
      Big.new(c: opc, t: true).ct_cria
    end

    desc 'recriare', 'atualiza rendas dos contratos ativos'
    # atualiza rendas dos contratos ativos
    def recriare
      Big.new.re_atualiza
    end

    desc 'work', 'carrega/apaga dados da folha calculo'
    option :s, type: :boolean, default: false, desc: 'apaga movimento similar (=data,=valor,<>descricao)'
    option :e, type: :boolean, default: false, desc: 'apaga movimento igual'
    option :v, banner: 'DATA', default: '',    desc: 'data valor para movimentos a carregar'
    option :g, banner: 'TAG',  default: '',    desc: 'classificacao para movimentos a carregar'
    # carrega/apaga dados da folha calculo
    def work
      Dir.glob("#{DR}/*.xlsx").each do |file|
        Folha.new(options.transform_keys(&:to_sym).merge(f: file, i: true)).processa_xls
      end
    end

    desc 'show', 'mostra dados da folha calculo'
    # mostra folha calculo
    def show
      Dir.glob("#{DR}/*.xlsx").each do |file|
        Folha.new(options.merge(f: file)).processa_xls
      end
    end

    default_task :show
  end
end
