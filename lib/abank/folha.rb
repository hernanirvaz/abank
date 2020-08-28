# frozen_string_literal: true

require('roo')

module Abank
  # acesso a folha calculo & base dados abank no bigquery
  class Folha < Big
    # @return [Roo::Excelx] folha calculo a processar
    attr_reader :folha

    # @return [Integer] numero conta associado a folha calculo
    # @example
    #   mov*.xlsx     --> 1 --> conta-corrente
    #   movCard*.xlsx --> 2 --> conta-cartao
    attr_reader :conta

    # @return [Array] row folha calculo em processamento
    attr_reader :rowfc

    # acesso a folha calculo & base dados abank no bigquery
    #
    # @param [Hash] opc opcoes trabalho
    # @option opc [String]  :f ('') folha calculo a processar
    # @option opc [Boolean] :s (false) apaga movimento similar? (mv)
    # @option opc [Boolean] :e (false) apaga movimento igual? (mv)
    # @option opc [Boolean] :i (false) insere movimento novo? (mv)
    # @option opc [String]  :v ('') data valor movimentos (mv)
    # @option opc [String]  :g ('') classificacao movimentos (mv)
    def initialize(opc = {})
      @opcao = super
      @folha = Roo::Spreadsheet.open(opc.fetch(:f))
      @conta = opc.fetch(:f).match?(/card/i) ? 2 : 1
      @opcao[:s] = opc.fetch(:s, false)
      @opcao[:e] = opc.fetch(:e, false)
      @opcao[:i] = opc.fetch(:i, false)
      @opcao[:v] = opc.fetch(:v, '')
      @opcao[:g] = opc.fetch(:g, '')
    end

    # carrega/mostra folha calculo
    def processa_xls
      n = 0
      folha.sheet(0).parse(header_search: ['Data Lanc.', 'Data Valor', 'Descrição', 'Valor']) do |r|
        n += 1
        puts n == 1 ? "\n#{folha.info}" : processa_linha(r)
      end
      return unless opcao[:i]

      # processa movimentos & atualiza rendas
      mv_delete.mv_insert.re_work
    end

    # processa linha folha calculo
    #
    # @param [Hash] linha da folha calculo em processamento
    # @return [String] texto informativo formatado da linha em processamento
    def processa_linha(linha)
      vars_xls(linha)
      # pesquisa existencia linha folha calculo no bigquery
      #  array.count = 0 ==> pode carregar esta linha
      #  array.count = 1 ==> mais testes necessarios
      #  array.count > 1 ==> nao pode carregar esta linha
      sql(sql_existe_mv, [{}, {}])
      if linha_naoexiste? then linha_base + values_mv
      elsif linha_existe? then linha_existe
      elsif linha_simila? then linha_similar
      else                     linha_multiplas
      end
    end

    # inicializa variavel para processar linha folha calculo
    #
    # @param (see processa_linha)
    def vars_xls(linha)
      @rowfc = linha.values
      @rowfc[2] = rowfc[2].strip
      @rowfc[3] = -1 * rowfc[3] if conta > 1
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
      bqres.each { |r| @mvkys += ",#{r[:ky]}" }
    end

    # @return [Boolean] linha folha calculo nao existe no bigquery?
    def linha_naoexiste?
      bqres.count.zero?
    end

    # @return [Boolean] linha folha calculo existe no bigquery?
    def linha_existe?
      bqres.count == 1 && bqres.first[:ds].strip == rowfc[2]
    end

    # @return [Boolean] linha folha calculo existe parecida no bigquery?
    def linha_simila?
      bqres.count == 1 && bqres.first[:ds].strip != rowfc[2]
    end

    # @return [String] sql para movimentos no bigquery
    def sql_existe_mv
      "select *,#{ky_mv} as ky from #{BD}.mv " \
       "where nc=#{conta} and dl='#{rowfc[0].strftime(DF)}' and vl=#{rowfc[3]}"
    end

    # obtem movimento (values.mv) para inserir
    #
    # @return [String] ' NOVO'
    def values_mv
      @mvvls += ",('#{rowfc[0].strftime(DF)}','#{dvc.strftime(DF)}','#{rowfc[2]}',#{rowfc[3]}" + values_mv_extra
      ' NOVO'
    end

    # @return [String] campos extra do movimento (values.mv) para inserir
    def values_mv_extra
      ",#{conta},#{dvc.year},#{dvc.month},'#{tpc}',#{ctc})"
    end

    # @return [Date] data valor corrigida
    def dvc
      opcao[:v].size.zero? ? rowfc[1] : Date.parse(opcao[:v])
    end

    # @return [String] classificacao do movimento (null --> classificacao automatica)
    def ctc
      opcao[:g].size.zero? ? 'null' : "'#{opcao[:g]}'"
    end

    # @return [String] tipo movimento c[redito] ou d[ebito]
    def tpc
      rowfc[3].positive? ? 'c' : 'd'
    end
  end
end
