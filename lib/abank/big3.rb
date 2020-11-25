# frozen_string_literal: true

module Abank
  # @see Big
  class Big
    # @return [Integer] movimento do contrato arrendamento em tratamento
    attr_reader :mvpos

    # (see CLI#recriare)
    def re_atualiza
      # [re]cria rendas novas/todas dos contratos ativos
      @ctlct = sql("select distinct ct from #{BD}.re")
      re_apaga.ct_dados.re_insert
    end

    # insere rendas associadas a lista contratos arrendamento no bigquery
    def re_insert
      @ctpos = 0
      vls = ct_rendas.join(',')
      if vls.empty?
        puts('NAO EXISTEM RENDAS NOVAS')
      else
        dml("insert #{BD}.re VALUES#{vls}")
        puts("RENDAS #{str_lc('')} CRIADAS #{bqnrs}")
      end
    end

    # apaga rendas da lista de contrato arrendamento
    #
    # @return [Big] acesso a base dados abank no bigquery
    def re_apaga
      return self if !opcao[:t] || ctlct.empty?

      # para nao apagar contrato arrendamento - somente as rendas
      opcao[:t] = false

      re_delete_dml
      self
    end

    # @return [Array<String>] lista rendas novas dum contrato arrendamento
    def re_rendas
      lre = []
      while mvpos < re_atual[:mv].size && re_saldo_mv?
        lre << re_nova_renda
        @mvpos += 1 unless re_saldo_mv?
      end
      lre
    end

    # @return [String] renda formatada (values.re)
    def re_nova_renda
      re_proximos_dados
      "('#{re_atual[:ct]}',#{ano},#{cnt},'#{re_atual_mv[:dl].strftime(DF)}',#{dias})"
    end

    # @return [Hash] dados contrato arrendamento (inclui lista movimentos novos)
    def re_proximos_dados
      # valor renda paga retirada do movimento
      re_atual_mv[:vl] -= re_atual[:vr]
      dre = cnt.zero? ? Date.new(ano, 1, 1) : Date.new(ano, cnt, 1) >> 1
      re_atual.merge!(ano: dre.year, cnt: dre.month)
    end

    # apaga rendas da lista de contratos arrendamento
    def re_delete_dml
      dml("delete from #{BD}.re where ct in(#{str_lc})#{opcao[:t] ? '' : ' and cnt>0'}")
      puts("RENDAS #{str_lc('')} APAGADAS #{bqnrs}")
    end

    # @return [Boolean] movimento com saldo suficiente?
    def re_saldo_mv?
      re_atual_mv[:vl] >= re_atual[:vr]
    end

    # @return [Hash] dados contrato arrendamento atual (inclui lista movimentos novos)
    def re_atual
      ctlct[ctpos]
    end

    # @return [Hash] movimento atual contrato arrendamento
    def re_atual_mv
      re_atual[:mv][mvpos]
    end

    # @return [Integer] ano da renda
    def ano
      re_atual[:ano]
    end

    # @return [Integer] numero da renda (0-12)
    def cnt
      re_atual[:cnt]
    end

    # @return [Integer] dias atraso/antecipo neste pagamento renda
    def dias
      re_atual_mv[:dl].mjd - (Date.new(ano, cnt, 1) >> (re_atual[:dc].month - 1)).mjd
    end
  end
end
