# frozen_string_literal: true

require('google/cloud/bigquery')

module Abank
  # @see Big
  class Big
    # @return [Hash] opcoes trabalho
    attr_reader :opcao

    # @return [Google::Cloud::Bigquery::Data] resultado do Structured Query Language (SQL) no bigquery
    attr_reader :bqres

    # @return [Integer] numero linhas afetadas pelo Data Manipulation Language (DML) no bigquery
    attr_reader :bqnrs

    # acesso a base dados abank no bigquery
    #
    # @param [Thor::CoreExt::HashWithIndifferentAccess] opcoes trabalho
    # @option opcoes [String]  :k ('') movimentos a apagar (keysin.mv)
    # @option opcoes [String]  :c ('') id contrato arrendamento (re)
    # @option opcoes [String]  :d ('') data inicio contrato arrendamento (re)
    # @option opcoes [Boolean] :t (false) trabalha todas as rendas? (re)
    # @return [Hash] opcoes trabalho
    def initialize(opcoes = {})
      @opcao = opcoes
      opcao
    end

    # @return [Google::Cloud::Bigquery] API bigquery
    def bqapi
      @bqapi ||= Google::Cloud::Bigquery.new
    end

    # @return [String] movimentos a apagar (keysin.mv)
    def mvkys
      opcao[:k][/([-+]*\d)+(,[-+]*\d+)*/].to_s
    end

    # apaga movimentos & suas rendas associadas no bigquery
    #
    # @return [Big] acesso a base dados abank no bigquery
    def mv_delete
      @ctlct = []
      unless mvkys.empty?
        # obtem lista contratos arrendamento associados aos movimentos a apagar
        @ctlct = sql("select distinct ct from #{BD}.mv where #{ky_mv} in(#{mvkys}) and substr(ct,1,1)='r'")

        # apaga todas as rendas dos contratos arrendamento associados aos movimentos a apagar
        opcao[:t] = true unless ctlct.empty?
        re_apaga.mv_delete_dml
      end
      self
    end

    # apaga movimentos no bigquery
    def mv_delete_dml
      dml("delete from #{BD}.mv where #{ky_mv} in(#{mvkys})")
      puts("MOVIMENTOS APAGADOS #{bqnrs}")
    end

    # (see CLI#tag)
    def mv_classifica
      dml("update #{BD}.mv set mv.ct=tt.nct from (select * from #{BD}.cl) as tt where #{ky_mv}=tt.ky")
      puts("MOVIMENTOS CLASSIFICADOS #{bqnrs}")
      @ctlct = sql("select distinct ct from #{BD}.re") if bqnrs.positive?
      self
    end

    # @return [String] expressao sql da chave de movimentos
    def ky_mv
      'FARM_FINGERPRINT(CONCAT(CAST(mv.nc as STRING),mv.ds,CAST(mv.dl as STRING),CAST(mv.vl as STRING)))'
    end

    # @param [String] cmd comando a executar
    # @return [Google::Cloud::Bigquery::QueryJob] tarefa SQL/DML no bigquery
    def job(cmd)
      bqjob = bqapi.query_job(cmd)
      bqjob.wait_until_done!
      err = bqjob.error
      puts(err['message']) if err
      bqjob
    end

    # executa Structured Query Language (SQL) no bigquery
    #
    # @param (see job)
    # @param [Array] erro resultado quando falha execucao
    # @return [Google::Cloud::Bigquery::Data] resultado do SQL
    def sql(cmd, erro = [])
      # se job.failed? executa job(cmd).data => StandardError
      @bqres = job(cmd).data
    rescue StandardError
      @bqres = erro
    end

    # executa Data Manipulation Language (DML) no bigquery
    #
    # @param (see job)
    # @return [Integer] numero rows afetadas pelo DML
    def dml(cmd)
      # se job.failed? executa Integer(nil) => StandardError
      @bqnrs = Integer(job(cmd).num_dml_affected_rows)
    rescue StandardError
      @bqnrs = 0
    end
  end
end
