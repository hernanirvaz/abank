# frozen_string_literal: true

require('google/cloud/bigquery')

module Abank
  BD = 'hernanilr.ab'

  # @see Big
  class Big
    DF = '%Y-%m-%d'

    # @return [Hash] opcoes trabalho
    attr_reader :opcao

    # @return [Google::Cloud::Bigquery::Data] resultado do Structured Query Language (SQL) no bigquery
    attr_reader :bqres

    # @return [Integer] numero linhas afetadas pelo Data Manipulation Language (DML) no bigquery
    attr_reader :bqnrs

    # @return [Integer] contrato arrendamento em tratamento
    attr_reader :ctpos

    # @return [Integer] movimento do contrato arrendamento em tratamento
    attr_reader :mvpos

    # acesso a base dados abank no bigquery
    #
    # @param [Thor::CoreExt::HashWithIndifferentAccess] opcoes trabalho
    # @option opcoes [String]  :k ('') movimentos a apagar (keysin.mv)
    # @option opcoes [String]  :c ('') id contrato arrendamento (re)
    # @option opcoes [String]  :d ('') data inicio contrato arrendamento (re)
    # @option opcoes [Boolean] :t (false) trabalha todas as rendas? (re)
    # @return [Hash] opcoes trabalho
    def initialize(opcoes = {})
      @opcao = opcoes
      opcao
    end

    # @return [Google::Cloud::Bigquery] API bigquery
    def bqapi
      @bqapi ||= Google::Cloud::Bigquery.new
    end

    # @return [String] movimentos a apagar (keysin.mv)
    def mvkys
      opcao[:k][/([-+]*\d)+(,[-+]*\d+)*/].to_s
    end

    # apaga movimentos & suas rendas associadas no bigquery
    #
    # @return [Big] acesso a base dados abank no bigquery
    def mv_delete
      @ctlct = []
      unless mvkys.empty?
        # obtem lista contratos arrendamento associados aos movimentos a apagar
        @ctlct = sql("select distinct ct from #{BD}.mv where #{ky_mv} in(#{mvkys}) and substr(ct,1,1)='r'")

        # apaga todas as rendas dos contratos arrendamento associados aos movimentos a apagar
        opcao[:t] = true unless ctlct.empty?
        re_apaga.mv_delete_dml
      end
      self
    end

    # apaga movimentos no bigquery
    def mv_delete_dml
      dml("delete from #{BD}.mv where #{ky_mv} in(#{mvkys})")
      puts("MOVIMENTOS APAGADOS #{bqnrs}")
    end

    # (see CLI#tag)
    def mv_classifica
      dml("update #{BD}.mv set mv.ct=tt.nct from (select * from #{BD}.cl) as tt where #{ky_mv}=tt.ky")
      puts("MOVIMENTOS CLASSIFICADOS #{bqnrs}")
      @ctlct = sql("select distinct ct from #{BD}.re") if bqnrs.positive?
      self
    end

    # @return [String] expressao sql da chave de movimentos
    def ky_mv
      'FARM_FINGERPRINT(CONCAT(CAST(mv.nc as STRING),mv.ds,CAST(mv.dl as STRING),CAST(mv.vl as STRING)))'
    end

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
      'select r1.ct,r0.dl dc,r1.ano,r1.cnt,r1.dl,CAST(REGEXP_EXTRACT(r1.ct,r"\d+") as numeric)/100 vr '\
        "from #{BD}.re r1 join hernanilr.ab.re r0 on (r0.ct=r1.ct and r0.cnt=0 and r1.cnt>0)"\
      " where r1.ct='#{opcao[:c]}' order by ano desc,cnt desc limit 1"
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

    # @param [String] cmd comando a executar
    # @return [Google::Cloud::Bigquery::QueryJob] tarefa SQL/DML no bigquery
    def job(cmd)
      bqjob = bqapi.query_job(cmd)
      bqjob.wait_until_done!
      err = bqjob.error
      puts(err['message']) if err
      bqjob
    end

    # executa Structured Query Language (SQL) no bigquery
    #
    # @param (see job)
    # @param [Array] erro resultado quando falha execucao
    # @return [Google::Cloud::Bigquery::Data] resultado do SQL
    def sql(cmd, erro = [])
      # se job.failed? executa job(cmd).data => StandardError
      @bqres = job(cmd).data
    rescue StandardError
      @bqres = erro
    end

    # executa Data Manipulation Language (DML) no bigquery
    #
    # @param (see job)
    # @return [Integer] numero rows afetadas pelo DML
    def dml(cmd)
      # se job.failed? executa Integer(nil) => StandardError
      @bqnrs = Integer(job(cmd).num_dml_affected_rows)
    rescue StandardError
      @bqnrs = 0
    end
  end
end
