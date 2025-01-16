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

      if linha_naoexiste?
        linha_base + values_mv
      elsif linha_existe?
        linha_existe
      elsif linha_simila?
        linha_similar
      else
        linha_multiplas
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
      "select *,#{BD}.ky(dl,dv,ds,vl,nc) ky from #{BD}.mv where nc=#{conta} and dl='#{rowfc[0].strftime(DF)}' and vl=#{rowfc[3]}"
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
