# frozen_string_literal: true

require 'google/cloud/bigquery'

module Abank
  # @see Big
  class Big
    DF = '%Y-%m-%d'

    # @return [Hash] opcoes trabalho
    attr_reader :opcao

    # @return [Google::Cloud::Bigquery] API bigquery
    attr_reader :bqapi

    # @return [Google::Cloud::Bigquery::QueryJob] job bigquery
    attr_reader :bqjob

    # @return [Google::Cloud::Bigquery::Data] resultado do select
    attr_reader :bqres

    # @return [Integer] numero linhas afetadas pela Data Manipulation Language (DML)
    attr_reader :bqnrs

    # @return [String] movimentos a inserir (values.mv)
    attr_reader :mvvls

    # @return [String] movimentos a apagar (keysin.mv)
    attr_reader :mvkys

    # acesso a base dados abank no bigquery
    #
    # @param [Hash] opc opcoes trabalho
    # @option opc [String]  :k ('') movimentos a apagar (keysin.mv)
    # @option opc [String]  :c ('') id contrato arrendamento (re)
    # @option opc [String]  :d ('') data inicio contrato arrendamento (re)
    # @option opc [Boolean] :t (false) trabalha todas as rendas? (re)
    # @return [Hash] opcoes trabalho
    def initialize(opc = {})
      @opcao = opc
      @bqapi = Google::Cloud::Bigquery.new
      @mvvls = ''
      @mvkys = opc.fetch(:k, '')
      @ctide = opc.fetch(:c, '')
      # p ['B', opcao]
      opcao
    end

    # (see CLI#tag)
    def mv_classifica
      dml('update hernanilr.ab.mv set mv.ct=tt.nct ' \
            'from (select * from hernanilr.ab.cl) as tt ' \
           "where #{ky_mv}=tt.ky")
      puts 'MOVIMENTOS CLASSIFICADOS ' + bqnrs.to_s
    end

    # apaga movimentos & suas rendas associadas no bigquery
    #
    # @return [Big] acesso a base dados abank no bigquery
    def mv_delete
      vars_mv_work
      if mvkys.size.positive?
        # obtem lista contratos arrendamento associados aos movimentos a apagar
        @ctlct = sel("select ct from hernanilr.ab.mv where #{ky_mv} in(#{mvkys}) and substr(ct,1,1)='r' group by 1")

        # apaga rendas associadas e depois movimentos
        @opcao[:t] = true
        lr_apaga.mv_delete_dml

        # para obrigar re_work a trabalhar com lista contratos (ctlct)
        @bqnrs = 0
      end
      self
    end

    # insere & classifica movimentos no bigquery
    #
    # @return [Big] acesso a base dados abank no bigquery
    def mv_insert
      if mvvls.size.positive?
        dml('insert hernanilr.ab.mv VALUES' + mvvls)
        puts 'MOVIMENTOS INSERIDOS ' + bqnrs.to_s
        mv_classifica if bqnrs.positive?
      end
      self
    end

    # inicializa variaveis para delete/insert movimentos
    def vars_mv_work
      @bqnrs = 0
      @ctlct = []
      @mvkys = mvkys[1..] if mvkys[0] == ','
      @mvvls = mvvls[1..] if mvvls[0] == ','
    end

    # apaga movimentos no bigquery
    def mv_delete_dml
      dml("delete from hernanilr.ab.mv where #{ky_mv} in(#{mvkys})")
      puts 'MOVIMENTOS APAGADOS ' + bqnrs.to_s
    end

    # @return [String] expressao sql da chave de movimentos
    def ky_mv
      'FARM_FINGERPRINT(CONCAT(CAST(mv.nc as STRING),mv.ds,CAST(mv.dl as STRING),CAST(mv.vl as STRING)))'
    end

    # cria job bigquery & verifica execucao
    #
    # @param [String] sql comando a executar
    # @return [Boolean] job ok?
    def job?(sql)
      # p sql
      @bqjob = bqapi.query_job(sql)
      @bqjob.wait_until_done!
      puts @bqjob.error['message'] if @bqjob.failed?
      @bqjob.failed?
    end

    # executa sql & devolve resultado do bigquery
    #
    # @param (see job?)
    # @param [Array] erro quando da erro no bigquery
    # @return [Google::Cloud::Bigquery::Data] resultado do sql
    def sel(sql, erro = [])
      @bqres = job?(sql) ? erro : bqjob.data
    end

    # executa Data Manipulation Language (DML) no bigquery
    #
    # @param (see job?)
    # @return [Integer] numero rows afetadas pelo dml
    def dml(sql)
      @bqnrs = job?(sql) ? 0 : bqjob.num_dml_affected_rows
    end
  end
end
