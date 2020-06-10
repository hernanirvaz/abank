# frozen_string_literal: true

# acesso a base dados abank no bigquery
class Abank::Big
  # @return [String] id contrato arrendamento
  attr_reader :ctide

  # @return [Array<Hash>] lista ids contratos arrendamento
  # @example
  #   [{ ct: 'r03000' }, ...]
  attr_reader :ctlct

  # @return [Array<Hash>] lista dados contrato arrendamento (inclui lista movimentos novos)
  # @example
  #   [{ct: 'r03000', dc: '2020-03-01', ano: 2020, cnt: 0, dl: '2020-03-01', mv: [{dl: '2020-03-02', vl: 30}, ...]  }]
  attr_reader :ctlcm

  # (see CLI#criact)
  def ct_cria
    if existe_contrato?
      @bqnrs = 1
      puts 'CONTRATO JA EXISTE'
    else
      dml('insert into hernanilr.ab.re ' + sql_contrato_mv)
      puts "CONTRATO #{ctide} " + (bqnrs.zero? ? 'NAO EXISTE' : 'INSERIDO')
    end
    return unless existem_rendas?

    # processa rendas associadas ao contrato arrendamento
    cm_cria.vr_cria.re_insert
  end

  # (see CLI#apagact)
  def ct_apaga
    @ctlct = [{ ct: ctide }]
    lc_apaga
  end

  # apaga rendas da lista de contrato arrendamento
  #
  # @return [Big] acesso a base dados abank no bigquery
  def lr_apaga
    return self unless opcao[:t] && ctlct.count.positive?

    # para nao apagar contrato arrendamento - somente as rendas
    @opcao[:t] = false

    lc_apaga
    self
  end

  # apaga rendas da lista de contratos arrendamento
  def lc_apaga
    dml("delete from hernanilr.ab.re where ct in(#{str_lc})#{opcao[:t] ? '' : ' and cnt>0'}")
    puts "RENDAS #{str_lc('')} APAGADAS " + bqnrs.to_s
  end

  # @return [String] texto formatado que representa lista de contratos arrendamento
  def str_lc(sep = "'")
    ctlct.map { |c| sep + c[:ct] + sep }.join(',')
  end

  # optem lista dados contrato arrendamento (inclui lista movimentos novos)
  #
  # @return [Big] acesso a base dados abank no bigquery
  def cm_cria
    @ctlcm = []
    ctlct.each do |c|
      @ctide = c[:ct]
      sel(sql_last_re)
      @ctlcm << bqres[0].merge({ mv: sel(sql_novo_mv(bqres[0][:dl])) })
    end
    self
  end

  # @return [Boolean] existem rendas para processar sim/nao?
  def existem_rendas?
    @ctlct = [{ ct: ctide }]
    bqnrs.positive? && opcao[:t]
  end

  # @return [Boolean] contrato arrendamento ja existe sim/nao?
  def existe_contrato?
    sel("select ct from hernanilr.ab.re where ct='#{ctide}' and cnt=0").count.positive?
  end

  # @return [String] sql para obter ultima renda do contrato arrendamento
  def sql_last_re
    'select ct,DATE_SUB(DATE_SUB(dl,INTERVAL dias DAY),INTERVAL IF(cnt=0,0,cnt-1) MONTH) as dc,ano,cnt,dl ' \
      "from hernanilr.ab.re where ct='#{ctide}' order by ano desc,cnt desc limit 1"
  end

  # @return [String] sql para obter movimentos novos (depois da ultima renda do contrato arrendamento)
  def sql_novo_mv(mdl)
    "select dl,vl from hernanilr.ab.mv where ct='#{ctide}' and dl>='#{(mdl + 1).strftime(DF)}' order by dl,dv"
  end

  # @return [String] sql para obter dados do inicio contrato arrendamento
  def sql_contrato_mv
    if opcao[:d].size.zero?
      'select ct,EXTRACT(YEAR FROM DATE_TRUNC(dl,MONTH)) as ano,0 as cnt,DATE_TRUNC(dl,MONTH) as dl,0 dias ' \
        "from hernanilr.ab.mv where ct='#{ctide}' order by dl limit 1"
    else
      "select '#{ctide}' as ct,EXTRACT(YEAR FROM DATE '#{opcao[:d]}') as ano,0 as cnt,DATE '#{opcao[:d]}' as dl,0 dias"
    end
  end
end
