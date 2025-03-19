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
      Big.new(options.transform_keys(&:to_sym)).mv_classifica.ct_dados.re_insert
    end

    desc 'amv', 'apaga movimentos keys|conta'
    option :k, banner: 'KEY[,KEY...]', default: '', desc: 'keys movimentos apagar'
    option :n, banner: 'CONTA', type: :numeric, default: 0, desc: 'conta movimentos apagar (>3 outras)'
    # apaga movimentos
    def amv
      Big.new(options.transform_keys(&:to_sym)).mv_delete.ct_dados.re_insert
    end

    desc 'sct', 'mostra contrato arrendamento'
    option :c, banner: 'CONTRATO', required: true, desc: 'identificador contrato arrendamento'
    option :t, type: :boolean, default: true, desc: 'mostra todas as rendas?'
    # mostra contrato arrendamento
    def sct
      Big.new(options.transform_keys(&:to_sym)).ct_mostra
    end

    desc 'act', 'apaga contrato arrendamento'
    option :c, banner: 'CONTRATO', required: true, desc: 'identificador contrato arrendamento'
    option :t, type: :boolean, default: false,     desc: 'apaga todas as rendas?'
    # apaga contrato arrendamento
    def act
      Big.new(options.transform_keys(&:to_sym)).ct_apaga
    end

    desc 'cct', 'cria contrato arrendamento'
    option :c, banner: 'CONTRATO', required: true, desc: 'identificador contrato arrendamento'
    option :d, banner: 'DATA', default: '',        desc: 'data contrato arrendamento'
    option :t, type: :boolean, default: true,      desc: 'cria todas as rendas?'
    # cria contrato arrendamento
    def cct
      Big.new(options.transform_keys(&:to_sym)).ct_cria
    end

    desc 'rct', 'atualiza rendas de contrato arrendamento'
    option :c, banner: 'CONTRATO', required: true, desc: 'identificador contrato arrendamento'
    # atualiza rendas de contrato arrendamento
    def rct
      opc = options[:c]
      Big.new(c: opc, t: false).ct_apaga
      Big.new(c: opc, t: true).ct_cria
    end

    desc 'rre', 'atualiza rendas dos contratos ativos'
    # atualiza rendas dos contratos ativos
    def rre
      Big.new.re_atualiza
    end

    desc 'ccc', 'cria classificador'
    option :c, banner: 'TAG', required: true, desc: 'identificador classificacao'
    option :p1, banner: 'P1s', required: true, type: :array, desc: 'array palavras p1'
    option :p2, banner: 'P2s', default: [], type: :array, desc: 'array palavras p2'
    option :t1, banner: 'T1s', default: [], type: :array, desc: 'array traducoes t1'
    # cria classificador
    def ccc
      Big.new(options.transform_keys(&:to_sym)).cc_cria.mv_classifica.ct_dados.re_insert
    end

    desc 'acc', 'apaga classificador'
    option :c, banner: 'TAG', required: true, desc: 'identificador classificacao'
    option :t, type: :boolean, default: false, desc: 're-classifica movimentos?'
    option :p1, banner: 'P1s', default: [], type: :array, desc: 'array palavras p1'
    option :p2, banner: 'P2s', default: [], type: :array, desc: 'array palavras p2'
    # apaga classificador
    def acc
      Big.new(options.transform_keys(&:to_sym)).cc_apaga.mv_classifica.ct_dados.re_insert
    end

    desc 'scc', 'mostra classificador'
    option :c, banner: 'TAG', default: '', desc: 'identificador classificacao'
    option :p1, banner: 'P1s', default: [], type: :array, desc: 'array palavras p1'
    option :p2, banner: 'P2s', default: [], type: :array, desc: 'array palavras p2'
    # mostra classificador
    def scc
      Big.new(options.transform_keys(&:to_sym)).cc_show
    end

    desc 'smc', 'mostra dados movimentos classificacao'
    option :c, banner: 'TAG', default: '', desc: 'identificador classificacao'
    option :n, banner: 'LIMIT', type: :numeric, default: 20, desc: 'numero movimentos a mostrar'
    # mostra dados classificacao
    def smc
      Big.new(options.transform_keys(&:to_sym).merge(p1: [], p2: [])).mc_show
    end

    desc 'work', 'carrega/apaga dados folha calculo'
    option :s, type: :boolean, default: false, desc: 'apaga movimento similar (=data,=valor,<>descricao)'
    option :e, type: :boolean, default: false, desc: 'apaga movimento igual'
    option :n, banner: 'CONTA', type: :numeric, default: 0, desc: 'conta destino (0 auto,1 corrente,2 cartao,3 chash,> outras)'
    option :v, banner: 'DATA', default: '',    desc: 'data lancamento para movimentos a carregar'
    option :g, banner: 'TAG',  default: '',    desc: 'classificacao para movimentos a carregar'
    # carrega/apaga dados folha calculo
    def work
      Dir.glob("#{DR}/*.xlsx").each do |file|
        Folha.new(options.transform_keys(&:to_sym).merge(f: file, i: true)).processa_xls
      end
    end

    desc 'show', 'mostra dados folha calculo'
    option :n, banner: 'CONTA', type: :numeric, default: 0, desc: 'conta destino (0 auto,1 corrente,2 cartao,3 chash,> outras)'
    # mostra folha calculo
    def show
      Dir.glob("#{DR}/*.xlsx").each do |file|
        Folha.new(options.transform_keys(&:to_sym).merge(f: file)).processa_xls
      end
    end

    default_task :show
  end
end
