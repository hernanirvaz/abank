# frozen_string_literal: true

require('roo')

module Abank
  class Folha < Big
    # @return [Array] row folha calculo em processamento
    # @return [String] movimentos a inserir (values.mv)
    attr_reader :rowfc, :mvvls

    # acesso a folha calculo & base dados abank no bigquery
    #
    # @param [String] xls folha calculo a processar
    # @param [Thor::CoreExt::HashWithIndifferentAccess] opcoes trabalho
    # @option opcoes [Boolean] :s (false) apaga movimento similar? (mv)
    # @option opcoes [Boolean] :e (false) apaga movimento igual? (mv)
    # @option opcoes [Boolean] :i (false) insere movimento novo? (mv)
    # @option opcoes [Integer] :n (0) conta dos movimentos (mv)
    # @option opcoes [String]  :v ('') data valor movimentos (mv)
    # @option opcoes [String]  :g ('') classificacao movimentos (mv)
    def initialize(opcoes = {})
      super
      @opcao = opcao.merge(
        s: opcoes.fetch(:s, false),
        e: opcoes.fetch(:e, false),
        i: opcoes.fetch(:i, false),
        n: opcoes.fetch(:n, 0),
        v: opcoes.fetch(:v, ''),
        g: opcoes.fetch(:g, ''),
        k: +'',
        f: opcoes[:f]
      )
      @mvvls = []
    end

    # carrega/mostra folha calculo
    def processa_xls
      puts("\n#{folha.info}")
      mvs = sql("select * from #{BD}.gmv where nc=@nc", nc: conta).group_by { |m| [m[:dl], m[:vl].to_f] }
      folha.sheet(0).parse(header_search: ['Data Lanc.', 'Data Valor', 'Descrição', 'Valor']) do |r|
        next unless valid?(r.values)

        @bqres = mvs[[rowfc[0], rowfc[3]]] || []
        if bqres.empty?
          puts(lnexi)
        elsif bqres.one? && bqres.first[:ds].strip == rowfc[2]
          puts(lexis)
        elsif bqres.one?
          puts(lsiml)
        else
          puts(lmult)
        end
      end
      return unless opcao[:i]

      # para nao apagar movimentos duma conta, por aqui somente com keys opcao[:k]
      opcao[:n] = 0
      mv_delete.mv_insert.ct_dados.re_insert
    end

    private

    # @return [Roo::Excelx] folha calculo a processar
    def folha
      @folha ||= Roo::Spreadsheet.open(opcao[:f])
    rescue StandardError
      raise("Erro ao abrir a folha de cálculo: #{opcao[:f]}")
    end

    # @return [Integer] obter numero conta a partir das opcoes
    def fconta
      return opcao[:n] if opcao[:n] > 2

      opcao[:f].match?(/card/i) ? 2 : 1
    end

    # @example
    #   mov*.xlsx     --> 1 --> conta-corrente
    #   movCard*.xlsx --> 2 --> conta-cartao
    #   opcao[:n]     --> 3 --> conta-cash
    #   opcao[:n]     --> n --> conta-outras
    # @return [Integer] numero conta associado a folha calculo
    def conta
      @conta ||= fconta
    end

    # @param [Array] row folha calculo em processamento
    # @return [Boolean] linha com valores correctos para processar?
    def valid?(row)
      return false unless row[0].is_a?(Date) && row[1].is_a?(Date)

      row[2] = row[2].to_s.strip.gsub("'", '').gsub('\\', '') # Descrição
      row[3] = row[3].to_f * (conta == 2 ? -1 : 1) # Valor
      @rowfc = row
      true
    rescue StandardError => e
      puts("Error processing row values: #{e.message}\nRow: #{row.inspect}")
      false
    end

    # @return [String] texto base formatado para display
    def lbase
      format('%<dt>10s %<v3>-34.34s %<v4>8.2f', dt: rowfc[0].strftime(DF), v3: rowfc[2], v4: rowfc[3])
    end

    # @return [String] novo texto base formatado para display
    def lnexi
      @mvvls << "('#{rowfc[0].iso8601}','#{dvc.iso8601}','#{rowfc[2]}',#{rowfc[3]},#{conta},#{dvc.year},#{dvc.month},'#{tpc}',#{ctc},null,null)"
      "#{lbase} NOVO"
    end

    # @return [String] texto linha existente formatada para display
    def lexis
      add_kys if opcao[:e]
      "#{lbase} EXIS #{format('%<v1>20d', v1: bqres.first[:ky])}"
    end

    # @return [String] texto linha similar formatada para display
    def lsiml
      add_kys if opcao[:s]
      "#{lbase} SIMI #{format('%<v1>20d %<v2>-34.34s', v1: bqres.first[:ky], v2: bqres.first[:ds].strip)}"
    end

    # @return [String] texto linha existencia multipla formatada para display
    def lmult
      "#{lbase} ML#{format('%<v0>2d %<v1>20d', v0: bqres.count, v1: bqres.first[:ky])}"
    end

    def add_kys
      opcao[:k] << bqres.each_with_object(+'') { |r, s| s << ",#{r[:ky]}" }
    end

    # @return [Date] data valor corrigida
    def dvc
      d = opcao[:v].to_s
      d.empty? ? rowfc[1] : Date.parse(d)
    rescue ArgumentError
      rowfc[1]
    end

    # @return [String] classificacao do movimento (null --> classificacao automatica)
    def ctc
      cmv = opcao[:g].to_s
      cmv.empty? ? 'null' : "'#{cmv}'"
    end

    # @return [String] tipo movimento c[redito] ou d[ebito]
    def tpc
      rowfc[3].positive? ? 'c' : 'd'
    end
  end
end
