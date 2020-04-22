# frozen_string_literal: true

require 'roo'
require 'google/cloud/bigquery'

module Abank
  DF = '%Y-%m-%d'

  # (see Bigquery)
  class Bigquery
    # @return [Google::Cloud::Bigquery] API bigquery
    attr_reader :apibq
    # @return [Roo::Excelx] folha calculo a processar
    attr_reader :folha
    # @return [Hash<Symbol, Boolean>] opcoes trabalho com linhas
    attr_reader :linha
    # @return [Integer] numero conta
    attr_reader :conta

    # @return [Array] row folha calculo em processamento
    attr_reader :row
    # @return [Google::Cloud::Bigquery::QueryJob] job bigquery
    attr_reader :job
    # @return (see sql_select)
    attr_reader :sql

    # @param [String] xls folha calculo para processar
    # @param [Hash<Symbol, Boolean>] ops opcoes trabalho com linhas
    # @option ops [Boolean] :s (false) apaga linha similar?
    # @option ops [Boolean] :e (false) apaga linha igual?
    # @option ops [Boolean] :m (false) apaga linhas existencia multipla?
    # @option ops [Boolean] :i (false) insere linha nova?
    # @return [Bigquery] acesso folhas calculo activobank
    #  & correspondente bigquery dataset
    def initialize(xls = '', ops = { s: false, e: false, m: false, i: false })
      # usa env GOOGLE_APPLICATION_CREDENTIALS para obter credentials
      # @see https://cloud.google.com/bigquery/docs/authentication/getting-started
      @apibq = Google::Cloud::Bigquery.new
      @folha = Roo::Spreadsheet.open(xls) if xls.size.positive?
      @linha = ops
      @conta = xls.match?(/card/i) ? 2 : 1
    end

    # cria job bigquery & verifica execucao
    #
    # @param [String] sql a executar
    # @return [Boolean] job ok?
    def job_bigquery?(sql)
      @job = apibq.query_job(sql)
      @job.wait_until_done!
      puts @job.error['message'] if @job.failed?
      @job.failed?
    end

    # cria Data Manipulation Language (DML) job bigquery
    #
    # @param (see job_bigquery?)
    # @return [Integer] numero linhas afetadas
    def dml(sql)
      job_bigquery?(sql) ? 0 : job.num_dml_affected_rows
    end

    # pesquisa existencia linha folha calculo no bigquery
    #
    # @return [Google::Cloud::Bigquery::Data] resultado do sql num array<hash>
    def sql_select
      # array.count = 0 ==> pode carregar esta linha
      # array.count = 1 ==> mais testes necessarios
      # array.count > 1 ==> nao carregar esta linha
      @sql = job_bigquery?('select * ' + sql_where) ? [{}, {}] : job.data
    end

    # @return [String] parte sql para processamento linhas similares
    def sql_where
      "from hernanilr.ab.mv where nc=#{conta}" \
      " and dl='#{row[0].strftime(DF)}'" \
      " and vl=#{row[3]}"
    end

    # (see CLI#classifica)
    def classifica
      return unless linha[:i]

      puts 'LINHAS CLASSIFICADAS ' +
           dml('update hernanilr.ab.mv set mv.ct=tt.nct' \
               '  from (select * from hernanilr.ab.cl) as tt' \
               ' where mv.dl=tt.dl and mv.dv=tt.dv' \
               '   and mv.ds=tt.ds and mv.vl=tt.vl').to_s
    end

    # @return [Integer] numero linhas inseridas
    def sql_insert
      return 1 unless linha[:i]

      dml('insert hernanilr.ab.mv(dl,dv,ds,vl,nc,ano,mes,ct,tp) VALUES(' \
        "'#{row[0].strftime(DF)}','#{row[1].strftime(DF)}','#{row[2]}'" +
        str_insert1)
    end

    # @return [String] campos extra da linha bigquery
    def str_insert1
      ",#{row[3]},#{conta}" + str_insert2
    end

    # @return [String] campos calculados da linha bigquery
    def str_insert2
      ",#{row[1].year},#{row[1].month},null,'#{row[3].positive? ? 'c' : 'd'}')"
    end

    # @return [Integer] numero linhas apagadas
    def sql_delete
      dml('delete ' + sql_where + " and ds='#{sql.first[:ds].strip}'")
    end
  end
end
