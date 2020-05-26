# frozen_string_literal: true

require 'google/cloud/bigquery'

# class Contrato
class Abank::Big
  DF = '%Y-%m-%d'

  # @return [Google::Cloud::Bigquery] API bigquery
  attr_reader :api
  # @return [Hash] opcoes trabalho com linhas
  attr_reader :opl

  # @return [Google::Cloud::Bigquery::QueryJob] job bigquery
  attr_reader :job
  # @return [Google::Cloud::Bigquery::Data] lista devolvida pelo select
  attr_reader :resultados

  # @param [Hash] ops opcoes trabalho
  # @option ops [Boolean] :s (false) apaga linha similar? (mv)
  # @option ops [Boolean] :e (false) apaga linha igual? (mv)
  # @option ops [Boolean] :m (false) apaga linhas existencia multipla? (mv)
  # @option ops [Boolean] :i (false) insere linha nova? (mv)
  # @option ops [String]  :v ('') data valor (mv)/data contrato (re)
  # @option ops [String]  :g ('') classificacao movimentos (mv)
  # @option ops [Boolean] :t (false) trabalha todoas as rendas? (re)
  # @option ops [String]  :k ('') keys movimentos a apagar (mv)
  # @return [Big] acesso bigquery dataset
  def initialize(ops = {})
    @opl = ops
    @api ||= Google::Cloud::Bigquery.new
    p ['Big', ops, api]
  end

  # (see CLI#classifica)
  def mv_classifica
    return unless opl[:i]

    i = dml('update hernanilr.ab.mv set mv.ct=tt.nct ' \
              'from (select * from hernanilr.ab.cl) as tt ' \
             'where mv.dl=tt.dl and mv.dv=tt.dv ' \
               'and mv.ds=tt.ds and mv.vl=tt.vl')
    puts 'LINHAS CLASSIFICADAS ' + i.to_s
    return unless i.positive?

    re_atualiza
  end

  # (see CLI#atualiza)
  def re_atualiza
    r = re_join(lista_ativos)
    if r.size.zero?
      puts 'NAO EXISTEM RENDAS NOVAS'
    else
      puts 'RENDAS CRIADAS ' + dml('insert hernanilr.ab.re VALUES' + r).to_s
    end
  end

  # (see CLI#apagamv)
  def mv_apaga
    e = ct_envolvidos
    i = dml(sql_apaga_mv)
    puts 'MOVIMENTOS APAGADOS ' + i.to_s
    return unless i.positive? && e.count.positive?

    e.map { |c| Contrato.new(c).re_apaga }

    re_atualiza
  end

  def ct_envolvidos
    sel(sql_sel_mv).group_by { |r| r[:ct] }
                   .delete_if { |k, _| !k || k[0] != 'r' }.keys
  end

  # @return [Array<Hash>] lista contratos com lista movimentos novos
  def lista_ativos
    sel(sql_ativos_re).map { |c| Contrato.new(c[:ct]).dados_contrato }.compact
  end

  # @param [Array<Hash>] lct lista contratos com lista movimentos novos
  # @return [String] row formatada das novas rendas para inserir bigquery
  def re_join(lct)
    lct.map { |c| Contrato::Rendas.new(c).rendas }.flatten(1).join(',')
  end

  def sql_ativos_re
    'SELECT ct from hernanilr.ab.re group by 1 order by 1'
  end

  def sql_sel_mv
    'select * ' + sql_where_mv
  end

  # @return [String] sql apaga movimentos
  def sql_apaga_mv
    'delete ' + sql_where_mv
  end

  # @return [String] parte sql para processamento movimentos
  def sql_where_mv
    "from hernanilr.ab.mv where #{sql_digest_mv} in(#{opl[:k]})"
  end

  def sql_digest_mv
    'FARM_FINGERPRINT(CONCAT(CAST(nc as STRING),' \
      'ds,CAST(dl as STRING),CAST(vl as STRING)))'
  end

  # cria job bigquery & verifica execucao
  #
  # @param [String] sql comando sql a executar
  # @return [Boolean] job ok?
  def job_bigquery?(sql)
    p sql
    @job = api.query_job(sql)
    @job.wait_until_done!
    puts @job.error['message'] if @job.failed?
    @job.failed?
  end

  # executa Data Manipulation Language (DML) job no bigquery
  #
  # @param (see job_bigquery?)
  # @return [Integer] numero linhas afetadas
  def dml(sql)
    job_bigquery?(sql) ? 0 : job.num_dml_affected_rows
  end

  # executa sql & devolve resultados do bigquery
  #
  # @param sql (see job_bigquery?)
  # @param [Array] arr resultado quando da erro no bigquery
  # @return [Google::Cloud::Bigquery::Data] resultado do sql num array<hash>
  def sel(sql, arr = [])
    @resultados = job_bigquery?(sql) ? arr : job.data
  end
end
