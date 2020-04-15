# frozen_string_literal: true

module Abank
  HT = ['Data Lanc.', 'Data Valor', 'Descrição', 'Valor'].freeze
  RF = '%<v3>-50.50s %<v4>8.2f'

  # folhas calculo comuns no bigquery
  class Bigquery
    # prepara linha folha calculo para processamento
    #
    # @param [Hash] has da linha em processamento
    def corrige_dados(has)
      @row = has.values
      @row[2] = row[2].strip
      @row[3] = -1 * row[3] if num > 1
    end

    # processa linhas folha calculo
    def processa
      n = 0
      book.sheet(0).parse(header_search: HT) do |r|
        n += 1
        puts n == 1 ? "\n" + book.info : processa_row(r)
      end
      sql_update
    end

    # mostra linhas folha calculo
    def show
      n = 0
      book.sheet(0).parse(header_search: HT) do |r|
        n += 1
        puts n == 1 ? "\n" + book.info : show_row(r)
      end
    end

    # processa linha folha calculo para arquivo
    #
    # @param (see corrige_dados)
    # @return [String] linha folha calculo processada
    def processa_row(has)
      corrige_dados(has)
      sql_select
      if rnaoexiste? then row_str + (sql_insert == 1 ? ' NOVA' : ' ERRO')
      elsif rexiste? then row_existente
      elsif rsimila? then row_similar
      end
    end

    # obtem linha folha calculo para apresentacao
    #
    # @param (see corrige_dados)
    # @return (see row_str)
    def show_row(has)
      corrige_dados(has)
      row_str
    end

    # @return [String] linha folha calculo formatada
    def row_str
      "#{row[0].strftime(DF)} #{row[1].strftime(DF)} " \
        "#{format(RF, v3: row[2], v4: row[3])}"
    end

    # @return [String] linha folha calculo similar
    def row_similar
      row_str + " PARECIDA #{sql.first[:ds].strip}"
    end

    # @return [String] linha folha calculo existente
    def row_existente
      row_str + ' EXISTE'
    end

    # @return [Boolean] linha folha calculo nao existe no bigquery?
    def rnaoexiste?
      sql.count.zero?
    end

    # @return [Boolean] linha folha calculo existe no bigquery?
    def rexiste?
      sql.count == 1 && sql.first[:ds].strip == row[2]
    end

    # @return [Boolean] linha folha calculo parecida no bigquery?
    def rsimila?
      sql.count == 1 && sql.first[:ds].strip != row[2]
    end
  end
end
