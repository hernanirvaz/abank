# frozen_string_literal: true

require('roo')

module Abank
  # @see Folha
  class Folha < Big
    # @return [Roo::Excelx] folha calculo a processar
    # attr_reader :folha

    # @return [Array] row folha calculo em processamento
    attr_reader :rowfc

    # @return [String] movimentos a inserir (values.mv)
    attr_reader :mvvls

    # acesso a folha calculo & base dados abank no bigquery
    #
    # @param [String] xls folha calculo a processar
    # @param [Thor::CoreExt::HashWithIndifferentAccess] opcoes trabalho
    # @option opcoes [Boolean] :s (false) apaga movimento similar? (mv)
    # @option opcoes [Boolean] :e (false) apaga movimento igual? (mv)
    # @option opcoes [Boolean] :i (false) insere movimento novo? (mv)
    # @option opcoes [String]  :v ('') data valor movimentos (mv)
    # @option opcoes [String]  :g ('') classificacao movimentos (mv)
    def initialize(opcoes = {})
      @opcao = super
      @opcao[:s] = opcoes.fetch(:s, false)
      @opcao[:e] = opcoes.fetch(:e, false)
      @opcao[:i] = opcoes.fetch(:i, false)
      @opcao[:v] = opcoes.fetch(:v, '')
      @opcao[:g] = opcoes.fetch(:g, '')
      # acumuladores necessitam init
      @opcao[:k] = ''
      @mvvls = ''
    end

    # @return [Roo::Excelx] folha calculo a processar
    def folha
      @folha ||= Roo::Spreadsheet.open(opcao[:f])
    end

    # carrega/mostra folha calculo
    def processa_xls
      puts("\n#{folha.info}")
      folha.sheet(0).parse(header_search: ['Data Lanc.', 'Data Valor', 'Descrição', 'Valor']) do |row|
        puts(processa_linha) if ok?(row)
      end
      return unless opcao[:i]

      # processa movimentos & atualiza rendas
      mv_delete.mv_insert.ct_dados.re_insert
    end

    # @return [Integer] numero conta associado a folha calculo
    # @example
    #   mov*.xlsx     --> 1 --> conta-corrente
    #   movCard*.xlsx --> 2 --> conta-cartao
    def conta
      opcao[:f].match?(/card/i) ? 2 : 1
    end

    # @param [Hash] linha da folha calculo em processamento
    # @return [Boolean] linha com valores para processar?
    def ok?(linha)
      @rowfc = linha.values
      return false if rowfc[0].is_a?(String)

      rowfc[2] = rowfc[2].strip
      rowfc[3] = -1 * rowfc[3] if conta > 1
      true
    end

    # @return [String] texto informativo formatado da linha processada
    def processa_linha
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

    # insere & classifica movimentos no bigquery
    #
    # @return [Big] acesso a base dados abank no bigquery
    def mv_insert
      unless mvvls.empty?
        @mvvls = mvvls[1..] if mvvls[0] == ','
        dml("insert #{BD}.mv VALUES#{mvvls}")
        puts("MOVIMENTOS INSERIDOS #{bqnrs}")
        mv_classifica if bqnrs.positive?
      end
      self
    end
  end
end
