# frozen_string_literal: true

module Abank
  # acesso a base dados abank no bigquery
  class Big
    DF = '%Y-%m-%d'

    # @return [Integer] contrato arrendamento em tratamento
    attr_reader :ctpos

    # (see CLI#criact)
    def ct_cria
      unless ct_existe?
        dml("insert into #{BD}.re #{sql_contrato_mv}")
        puts("CONTRATO #{opcao[:c]} #{bqnrs.zero? ? 'NAO ' : ''}INSERIDO")
      end
      # processar rendas sim/nao?
      return unless bqnrs.positive? && opcao[:t]

      # processa rendas associadas ao contrato arrendamento
      ct_dados.re_insert
    end

    # @return [Boolean] contrato arrendamento ja existe sim/nao?
    def ct_existe?
      @ctlct = [{ ct: opcao[:c] }]
      vaz = sql("select ct from #{BD}.re where ct in(#{str_lc}) and cnt=0").empty?
      unless vaz
        @bqnrs = 1
        puts('CONTRATO JA EXISTE')
      end
      !vaz
    end

    # (see CLI#apagact)
    def ct_apaga
      @ctlct = [{ ct: opcao[:c] }]
      re_delete_dml
    end

    # optem lista dados contrato arrendamento (inclui lista movimentos novos)
    #
    # @return [Big] acesso a base dados abank no bigquery
    def ct_dados
      ctlct.map! do |ctr|
        opcao[:c] = ctr[:ct]
        lre = sql(sql_last_re)[0]
        ctr.merge(lre, mv: sql(sql_novo_mv(lre[:dl])))
      end
      self
    end

    # @param [Array] are lista rendas novas atual
    # @return [Array<String>] lista rendas novas duma lista contratos arrendamento
    def ct_rendas(lre = [])
      while ctpos < ctlct.size
        @mvpos = 0
        lre += re_rendas
        @ctpos += 1
      end
      lre
    end

    # @example sem dados movimentos
    #   [{ ct: 'r03000' }, ...]
    # @example com dados movimentos
    #   [{ct: 'r03000', dc: '2020-03-01', ano: 2020, cnt: 0, dl: '2020-03-01', mv: [{dl: '2020-03-02', vl: 30}, ...]  }]
    # @return [Array<Hash>] lista dados contrato arrendamento (inclui lista movimentos novos)
    def ctlct
      @ctlct ||= []
    end

    # @return [String] texto formatado que representa lista de contratos arrendamento
    def str_lc(sep = "'")
      ctlct.map { |cid| sep + cid[:ct] + sep }.join(',')
    end

    # @return [String] sql para obter ultima renda do contrato arrendamento
    def sql_last_re
      'select ct,DATE_SUB(DATE_SUB(dl,INTERVAL dias DAY),INTERVAL IF(cnt=0,0,cnt-1) MONTH) dc,ano,cnt,dl'\
            ',CAST(REGEXP_EXTRACT(ct,r"\d+") as numeric)/100 vr '\
        "from #{BD}.re where ct='#{opcao[:c]}' order by ano desc,cnt desc limit 1"
    end

    # @return [String] sql para obter movimentos novos (depois da ultima renda do contrato arrendamento)
    def sql_novo_mv(mdl)
      "select dl,vl from #{BD}.mv where ct='#{opcao[:c]}' and dl>='#{(mdl + 1).strftime(DF)}' order by dl,dv"
    end

    # @return [String] sql para obter dados do inicio contrato arrendamento
    def sql_contrato_mv
      cti = opcao[:c]
      dat = opcao[:d]
      if dat.empty?
        'select ct,EXTRACT(YEAR FROM DATE_TRUNC(dl,MONTH)) ano,0 cnt,DATE_TRUNC(dl,MONTH) dl,0 dias '\
          "from #{BD}.mv where ct='#{cti}' order by dl limit 1"
      else
        "select '#{cti}' ct,EXTRACT(YEAR FROM DATE '#{dat}') ano,0 cnt,DATE '#{dat}' dl,0 dias"
      end
    end
  end
end
