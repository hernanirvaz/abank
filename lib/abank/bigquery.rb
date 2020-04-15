# frozen_string_literal: true

require 'roo'
require 'google/cloud/bigquery'

module Abank
  DF = '%Y-%m-%d'

  # folhas calculo comuns no bigquery
  class Bigquery
    # @return [Roo::Excelx] folha calculo a processar
    attr_reader :book
    # @return [Array] row folha calculo em processamento
    attr_reader :row
    # @return [Google::Cloud::Bigquery] API bigquery
    attr_reader :api
    # @return [Google::Cloud::Bigquery::QueryJob] job bigquery
    attr_reader :job
    # @return (see sql_select)
    attr_reader :sql
    # @return [Integer] numero conta
    attr_reader :num

    # permite processa folhas calculo comuns no bigquery
    #
    # @param [String] xls folha calculo para processar
    # @return [Bigquery] acesso folha calculo & bigquery
    def initialize(xls)
      @book = Roo::Spreadsheet.open(xls) if xls.size.positive?
      @num = xls.match?(/card/i) ? 2 : 1
      # usa env GOOGLE_APPLICATION_CREDENTIALS para obter credentials
      # @see https://cloud.google.com/bigquery/docs/authentication/getting-started
      @api = Google::Cloud::Bigquery.new
    end

    # cria job bigquery & verifica execucao
    #
    # @param [String] sql para executar
    # @return [Boolean] job ok?
    def job_bigquery?(sql)
      @job = api.query_job(sql)
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

    # cria sql job bigquery com resultados
    #
    # @return [Array<Hash>] resultado sql
    def sql_select
      s = "select * from ab.mv where nc=#{num}" \
            " and dl='#{row[0].strftime(DF)}'" \
            " and vl=#{row[3]}"

      # se array.count > 1 => nao fazer nada
      @sql = job_bigquery?(s) ? [{}, {}] : job.data
    end

    # classifica linhas
    def sql_update
      puts 'LINHAS CLASSIFICADAS ' +
           dml('update ab.mv set mv.ct=tt.nct' \
               ' from (select * from ab.cl) as tt ' \
               'where mv.dl=tt.dl and mv.dv=tt.dv' \
               ' and mv.ds=tt.ds and mv.vl=tt.vl').to_s
    end

    # @return [Integer] numero linhas inseridas
    def sql_insert
      dml('insert ab.mv(dl,dv,ds,vl,nc,ano,mes,ct,tp) VALUES(' \
        "'#{row[0].strftime(DF)}','#{row[1].strftime(DF)}','#{row[2]}'," \
        "#{row[3]},#{num}" + sql_insert_calculado)
    end

    # @return [String] campos calculados da linha bigquery
    def sql_insert_calculado
      ",#{row[1].year},#{row[1].month},null,'#{row[3].positive? ? 'c' : 'd'}')"
    end
  end
end
