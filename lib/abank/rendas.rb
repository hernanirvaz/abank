# frozen_string_literal: true

module Abank
  # @see Big
  class Big
    # @return [Integer] ano renda em tratamento
    attr_reader :reano

    # @return [Integer] mes renda em tratamento
    attr_reader :repos

    # @return [Float] valor renda mensal
    attr_reader :revre

    # @return [String] rendas a inserir (values.re)
    attr_reader :revls

    # @return [Integer] movimento em tratamento
    attr_reader :mvpos

    # @return [Date] data lancamento movimento em tratamento
    attr_reader :mvdlm

    # @return [Float] valor movimento em tratamento
    attr_reader :mvvlm

    # (see CLI#recriare)
    def re_atualiza
      # obtem contratos ativos
      @ctlct = sql("SELECT ct from #{BD}.re group by 1")

      # [re]cria rendas [novas|todas]
      lr_apaga.cm_cria.vr_cria.re_insert
    end

    # cria rendas associadas a lista ids contratos arrendamento
    def re_work
      bqnrs.zero? || ctlct.count.positive? ? cm_cria.vr_cria.re_insert : re_atualiza
    end

    # obtem rendas a inserir (values.re)
    #
    # @return [Big] acesso a base dados abank no bigquery
    def vr_cria
      @revls = ctlcm.map { |c| rendas_novas(c) }.flatten(1).join(',')
      self
    end

    # insere rendas no bigquery
    def re_insert
      if revls.size.zero?
        puts('NAO EXISTEM RENDAS NOVAS')
      else
        dml("insert #{BD}.re VALUES#{revls}")
        puts("RENDAS #{str_lc('')} CRIADAS " + bqnrs.to_s)
      end
    end

    # @param [Hash] cmv dados contrato arrendamento (inclui lista movimentos novos)
    # @return [Array<String>] lista rendas novas dum contrato arrendamento (values.re)
    def rendas_novas(cmv)
      return [] unless cmv[:mv].count.positive?

      vars_re(cmv)
      r = []
      while mvvlm >= revre && mvpos < cmv[:mv].count
        r << nova_re(cmv)
        proximo_mv(cmv)
      end
      r
    end

    # inicializa variaveis para processar rendas do contrato arrendamento
    # @param (see rendas_novas)
    def vars_re(cmv)
      @reano = cmv[:ano]
      @repos = cmv[:cnt]
      @revre = Float(cmv[:ct][/\d+/]) / 100
      @mvpos = 0
      vars_re_mv(cmv)
    end

    # inicializa variaveis para processar movimentos associados ao contrato arrendamento
    # @param (see rendas_novas)
    def vars_re_mv(cmv)
      @mvdlm = cmv[:mv][mvpos][:dl]
      @mvvlm = cmv[:mv][mvpos][:vl]
    end

    # @param (see rendas_novas)
    # @return [String] renda formatada (values.re)
    def nova_re(cmv)
      # inicializa proxima renda
      if repos == 12
        @repos = 1
        @reano += 1
      else
        @repos += 1
      end
      "('#{cmv[:ct]}',#{reano},#{repos},'#{mvdlm.strftime(DF)}',#{dias(cmv)})"
    end

    # @param (see rendas_novas)
    # @return [Integer] dias atraso no pagamento da renda
    def dias(cmv)
      mvdlm.mjd - (Date.new(reano, repos, 1) >> (cmv[:dc].month - 1)).mjd
    end

    # inicializa variaveis para processar proximo movimento
    # @param (see rendas_novas)
    def proximo_mv(cmv)
      # valor renda paga retirado do valor do movimento
      @mvvlm -= revre
      return unless mvvlm < revre

      # avanca na lista de movimentos
      @mvpos += 1
      return unless mvpos < cmv[:mv].count

      vars_re_mv(cmv)
    end
  end
end
