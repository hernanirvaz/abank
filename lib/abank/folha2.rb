# frozen_string_literal: true

module Abank
  # acesso a folha calculo & base dados abank no bigquery
  class Folha < Big
    # @return [Boolean] linha folha calculo nao existe no bigquery?
    def linha_naoexiste?
      bqres.empty?
    end

    # @return [Boolean] linha folha calculo existe no bigquery?
    def linha_existe?
      bqres.count == 1 && bqres.first[:ds].strip == rowfc[2]
    end

    # @return [Boolean] linha folha calculo existe parecida no bigquery?
    def linha_simila?
      bqres.count == 1 && bqres.first[:ds].strip != rowfc[2]
    end

    # @return [String] texto base formatado para display
    def linha_base
      "#{rowfc[0].strftime(DF)} #{format('%<v3>-34.34s %<v4>8.2f', v3: rowfc[2], v4: rowfc[3])}"
    end

    # @return [String] texto linha existente formatada para display
    def linha_existe
      add_kys if opcao[:e]
      "#{linha_base} EXIS #{format('%<v1>20d', v1: bqres.first[:ky])}"
    end

    # @return [String] texto linha similar formatada para display
    def linha_similar
      add_kys if opcao[:s]
      "#{linha_base} SIMI #{format('%<v1>-20.20s', v1: bqres.first[:ds].strip)}"
    end

    # @return [String] texto linha existencia multipla formatada para display
    def linha_multiplas
      "#{linha_base} MULT(#{bqres.count})"
    end

    # obtem chaves movimento (keysin.mv) para apagar
    def add_kys
      bqres.each { |row| opcao[:k] += ",#{row[:ky]}" }
    end

    # @return [String] sql para movimentos no bigquery
    def sql_existe_mv
      "select *,#{ky_mv} as ky from #{BD}.mv where nc=#{conta} and dl='#{rowfc[0].strftime(DF)}' and vl=#{rowfc[3]}"
    end

    # obtem movimento (values.mv) para inserir
    #
    # @return [String] ' NOVO'
    def values_mv
      @mvvls += ",('#{rowfc[0].strftime(DF)}','#{dvc.strftime(DF)}','#{rowfc[2]}',#{rowfc[3]}"\
                ",#{conta},#{dvc.year},#{dvc.month},'#{tpc}',#{ctc})"
      ' NOVO'
    end

    # @return [Date] data valor corrigida
    def dvc
      dvl = opcao[:v]
      dvl.empty? ? rowfc[1] : Date.parse(dvl)
    end

    # @return [String] classificacao do movimento (null --> classificacao automatica)
    def ctc
      cmv = opcao[:g]
      cmv.empty? ? 'null' : "'#{cmv}'"
    end

    # @return [String] tipo movimento c[redito] ou d[ebito]
    def tpc
      rowfc[3].positive? ? 'c' : 'd'
    end
  end
end
