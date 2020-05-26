# frozen_string_literal: true

require 'roo'

# class Big::Contrato
class Abank::Big::Folha < Abank::Big
  HT = ['Data Lanc.', 'Data Valor', 'Descrição', 'Valor'].freeze
  RF = '%<v3>-34.34s %<v4>8.2f'
  EN = ' %<v1>20d'
  ES = ' %<v1>-20.20s'

  # @return [Roo::Excelx] folha calculo a processar
  attr_reader :folha
  # @return [Integer] numero conta
  attr_reader :conta

  # @return [Array] row folha calculo em processamento
  attr_reader :row

  def initialize(xls, ops = { s: false, e: false, m: false, i: false,
                              v: '', g: '' })
    p ['Folha', xls, ops]
    @folha = Roo::Spreadsheet.open(xls) if xls.size.positive?
    @conta = xls.match?(/card/i) ? 2 : 1
    super(ops)
  end

  # processa linhas folha calculo & classifica bigquery
  def processa_folha
    n = 0
    folha.sheet(0).parse(header_search: HT) do |r|
      n += 1
      puts n == 1 ? "\n" + folha.info : processa_row(r)
    end
    mv_classifica
  end

  # processa linha folha calculo para arquivo
  #
  # @param (see corrige_hash)
  # @return [String] texto informativo do processamento
  def processa_row(has)
    corrige_hash(has)
    # pesquisa existencia linha folha calculo no bigquery
    #  array.count = 0 ==> pode carregar esta linha
    #  array.count = 1 ==> mais testes necessarios
    #  array.count > 1 ==> nao pode carregar esta linha
    sel(sql_sel_mv, [{}, {}])
    if row_naoexiste? then row_str + (insert_mv == 1 ? ' NOVA' : ' ERRO')
    elsif row_simila? then row_similar
    elsif row_existe? then row_existente
    else                   row_multiplas
    end
  end

  # corrige linha folha calculo para processamento
  #
  # @param [Hash] has da linha em processamento
  def corrige_hash(has)
    @row = has.values
    @row[2] = row[2].strip
    @row[3] = -1 * row[3] if conta > 1
  end

  # @return [String] linha folha calculo formatada
  def row_str
    "#{row[0].strftime(DF)} #{format(RF, v3: row[2], v4: row[3])}"
  end

  # @return [String] linha folha calculo similar
  def row_similar
    d = opl[:s] ? delete_mv : 0
    row_str + ' SIMI' + str_apagadas(d) + str_extra_s(resultados.first[:ds])
  end

  # @return [String] linha folha calculo existente
  def row_existente
    d = opl[:e] ? delete_mv : 0
    row_str + ' EXIS' + str_apagadas(d) + str_extra_n(resultados.first[:ky])
  end

  def str_extra_s(ext)
    format(ES, v1: ext.strip)
  end

  def str_extra_n(ext)
    format(EN, v1: ext)
  end

  # @return [String] linha folha calculo existencia multipla
  def row_multiplas
    d = opl[:m] ? delete_mv : 0
    row_str + ' M(' + resultados.count.to_s + ')' + str_apagadas(d)
  end

  # @param [Integer] numero linhas apagadas
  # @return [String] texto formatado linhas apagadas
  def str_apagadas(num)
    num.positive? ? ' A(' + num.to_s + ')' : ''
  end

  # @return [Boolean] linha folha calculo nao existe no bigquery?
  def row_naoexiste?
    resultados.count.zero?
  end

  # @return [Boolean] linha folha calculo existe no bigquery?
  def row_existe?
    resultados.count == 1 && resultados.first[:ds].strip == row[2]
  end

  # @return [Boolean] linha folha calculo existe parecida no bigquery?
  def row_simila?
    resultados.count == 1 && resultados.first[:ds].strip != row[2]
  end

  def sql_sel_mv
    "select *,#{sql_digest_mv} as ky " + sql_where_mv
  end

  # @return [String] parte sql para processamento movimentos
  def sql_where_mv
    "from hernanilr.ab.mv where nc=#{conta} " \
     "and dl='#{row[0].strftime(DF)}' " \
     "and vl=#{row[3]}"
  end

  # @return [Integer] numero linhas inseridas
  def insert_mv
    return 1 unless opl[:i]

    dml('insert hernanilr.ab.mv(dl,dv,ds,vl,nc,ano,mes,ct,tp) VALUES(' \
        "'#{row[0].strftime(DF)}','#{dvc.strftime(DF)}','#{row[2]}',#{row[3]}" +
      str_ins_pc)
  end

  # @return [Date] data valor corrigida
  def dvc
    opl[:v].size.zero? ? row[1] : Date.parse(opl[:v])
  end

  # @return [String] campos extra da linha bigquery
  def str_ins_pc
    ",#{conta},#{dvc.year},#{dvc.month},#{ctc},'#{tpc}')"
  end

  def ctc
    opl[:g].size.zero? ? 'null' : ("'" + opl[:g] + "'")
  end

  def tpc
    row[3].positive? ? 'c' : 'd'
  end

  # @return [Integer] numero linhas apagadas
  def delete_mv
    dml('delete ' + sql_where_mv + " and ds='#{resultados.first[:ds]}'")
  end
end
