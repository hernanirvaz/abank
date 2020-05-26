# frozen_string_literal: true

# class Big::Contrato
class Abank::Big::Contrato < Abank::Big
  # @return [String] identificador contrato arrendamento
  attr_reader :rct

  # {:ct=>"r03000"}
  def initialize(con, ops = { t: false, v: '' })
    p ['Contrato', con, ops]
    @rct = con
    super(ops)
  end

  # (see CLI#cria)
  def re_cria
    if existe_contrato?
      i = 0
      puts 'JA EXISTE CONTRATO'
    else
      i = dml('insert into hernanilr.ab.re ' + sql_contrato_mv)
      puts i.zero? ? 'NAO EXISTE CONTRATO' : "CONTRATO #{rct} INSERIDO"
    end
    return unless i.positive? && opl[:t]

    re_atualiza
  end

  # (see CLI#apagare)
  def re_apaga
    puts "RENDAS #{rct} APAGADAS " + dml(sql_apaga_re).to_s
  end

  # @return [Hash] dados contrato & movimentos novos
  def dados_contrato
    c = sel(sql_last_re).first
    sel(sql_novo_mv(c[:dl]))
    return unless resultados.count.positive?

    { mv: resultados }.merge(c)
  end

  def sql_last_re
    'select ct,DATE_SUB(DATE_SUB(dl,INTERVAL dias DAY)' \
          ',INTERVAL IF(cnt=0,0,cnt-1) MONTH) as dc,ano,cnt,dl ' \
      "from hernanilr.ab.re where ct='#{rct}' " \
    'order by ano desc,cnt desc limit 1'
  end

  def sql_novo_mv(mdl)
    "select dl,vl from hernanilr.ab.mv where ct='#{rct}' " \
       "and dl>='#{(mdl + 1).strftime(DF)}' order by dl,dv"
  end

  def existe_contrato?
    sel(sql_contrato_re).count.positive?
  end

  def sql_contrato_re
    "select * from hernanilr.ab.re where ct='#{rct}' and cnt=0"
  end

  # @return [String] sql obtem dados inicio contrato arrendamento
  def sql_contrato_mv
    if opl[:v].size.zero?
      'select ct,EXTRACT(YEAR FROM DATE_TRUNC(dl,MONTH)) as ano,0 as cnt' \
                                 ',DATE_TRUNC(dl,MONTH) as dl,0 dias ' \
        "from hernanilr.ab.mv where ct='#{rct}' order by dl limit 1"
    else
      "select '#{rct}' as ct" \
            ",EXTRACT(YEAR FROM DATE '#{opl[:v]}') as ano,0 as cnt" \
                              ",DATE '#{opl[:v]}' as dl,0 dias "
    end
  end

  def sql_apaga_re
    "delete from hernanilr.ab.re where ct='#{rct}'" +
      (opl[:t] ? '' : ' and cnt>0')
  end
end
