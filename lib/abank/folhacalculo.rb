# frozen_string_literal: true

module Abank
  HT = ['Data Lanc.', 'Data Valor', 'Descrição', 'Valor'].freeze
  RF = '%<v3>-35.35s %<v4>8.2f'

  # classifica & arquiva dados das folhas calculo activobank no bigquery
  class Bigquery
    # corrige linha folha calculo para processamento
    #
    # @param [Hash] has da linha em processamento
    def corrige_hash(has)
      @row = has.values
      @row[2] = row[2].strip
      @row[3] = -1 * row[3] if conta > 1
    end

    # processa linhas folha calculo & classifica bigquery
    def processa
      n = 0
      folha.sheet(0).parse(header_search: HT) do |r|
        n += 1
        puts n == 1 ? "\n" + folha.info : processa_row(r)
      end
      classifica
    end

    # processa linha folha calculo para arquivo
    #
    # @param (see corrige_hash)
    # @return [String] texto informativo do processamento
    def processa_row(has)
      corrige_hash(has)
      sql_select
      if row_naoexiste? then row_str + (sql_insert == 1 ? ' NOVA' : ' ERRO')
      elsif row_simila? then row_similar
      elsif row_existe? then row_existente
      else                   row_multiplas
      end
    end

    # @return [String] linha folha calculo formatada
    def row_str
      "#{row[0].strftime(DF)} #{row[1].strftime(DF)} " \
        "#{format(RF, v3: row[2], v4: row[3])}"
    end

    # @return [String] linha folha calculo similar
    def row_similar
      d = linha[:s] ? sql_delete : 0
      row_str + ' SIMILAR' + str_apagadas(d) + sql.first[:ds].strip
    end

    # @return [String] linha folha calculo existente
    def row_existente
      d = linha[:e] ? sql_delete : 0
      row_str + ' EXISTENTE' + str_apagadas(d)
    end

    # @return [String] linha folha calculo existencia multipla
    def row_multiplas
      d = linha[:m] ? sql_delete : 0
      row_str + ' MULTIPLAS ' + sql.count.to_s + str_apagadas(d)
    end

    # @param [Integer] numero linhas apagadas
    # @return [String] texto formatado linhas apagadas
    def str_apagadas(num)
      num.positive? ? ' & ' + num.to_s + ' APAGADA(S) ' : ' '
    end

    # @return [Boolean] linha folha calculo nao existe no bigquery?
    def row_naoexiste?
      sql.count.zero?
    end

    # @return [Boolean] linha folha calculo existe no bigquery?
    def row_existe?
      sql.count == 1 && sql.first[:ds].strip == row[2]
    end

    # @return [Boolean] linha folha calculo existe parecida no bigquery?
    def row_simila?
      sql.count == 1 && sql.first[:ds].strip != row[2]
    end
  end
end
