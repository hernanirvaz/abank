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
      opcao[:k].to_s.scan(/[-+]?\d+/).join(',')
    end

    # apaga movimentos & suas rendas associadas no bigquery
    #
    # @return [Big] acesso a base dados abank no bigquery
    def mv_delete
      @ctlct = []
      unless mvkys.empty?
        # obtem lista contratos arrendamento associados aos movimentos a apagar
        @ctlct = sql("select distinct ct from #{BD}.gmr where ky in(#{mvkys})")

        re_apaga.mv_delete_dml
      end
      self
    end

    # apaga movimentos no bigquery
    def mv_delete_dml
      dml("delete from #{BD}.mv where #{BD}.ky(dl,dv,ds,vl,nc,ex) in(#{mvkys})")
      puts("MOVIMENTOS APAGADOS #{bqnrs}")
    end

    # (see CLI#tag)
    def mv_classifica
      @ctlct = []
      stp("call #{BD}.uct()")
      puts("MOVIMENTOS CLASSIFICADOS #{bqnrs}")
      @ctlct = sql("select ct from #{BD}.ca") if bqnrs.positive?
      self
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
      vaz = sql("select ct from #{BD}.ca where ct in(#{str_lc})").empty?
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
        lre = sql(sql_last_re).first
        lre[:dl] += -1 if lre[:cnt].zero?
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
      "select * from #{BD}.glr where ct='#{opcao[:c]}' order by ano desc,cnt desc limit 1"
    end

    # @return [String] sql para obter movimentos novos (depois da ultima renda do contrato arrendamento)
    def sql_novo_mv(mdl)
      "select * from #{BD}.gmn where ct='#{opcao[:c]}' and dl>='#{(mdl + 1).strftime(DF)}' order by 1,2"
    end

    # @return [String] sql para obter dados do inicio contrato arrendamento
    def sql_contrato_mv
      cti = opcao[:c]
      dat = opcao[:d]
      if dat.empty?
        'select ct,EXTRACT(YEAR FROM DATE_TRUNC(dl,MONTH)) ano,0 cnt,DATE_TRUNC(dl,MONTH) dl,0 dias ' \
          "from #{BD}.mv where ct='#{cti}' order by dl limit 1"
      else
        "select '#{cti}' ct,EXTRACT(YEAR FROM DATE '#{dat}') ano,0 cnt,DATE '#{dat}' dl,0 dias"
      end
    end

    # (see CLI#recriare)
    def re_atualiza
      # [re]cria rendas novas/todas dos contratos ativos
      @ctlct = sql("select ct from #{BD}.ca")
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
      return self if ctlct.empty?

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
      dre = cnt.zero? ? Date.new(re_atual[:dc].year, re_atual[:dc].month, 1) : Date.new(ano, cnt, 1) >> 1
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
      re_atual_mv[:dl].mjd - Date.new(ano, cnt, 1).mjd
    end

    # @param [String] cmd comando a executar
    # @return [Google::Cloud::Bigquery::QueryJob] tarefa SQL/DML no bigquery
    def job(cmd)
      bqjob = bqapi.query_job(cmd)
      bqjob.wait_until_done!
      puts(bqjob.error['message']) if bqjob.failed?
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

    # executa Stored Procedure (STP) with DML operations no bigquery
    #
    # @param (see job)
    # @return [Integer] numero rows afetadas pelo STP
    def stp(cmd)
      # last command STP=SELECT @@row_count AS rows_affected;
      # se job.failed? executa Integer(nil) => StandardError
      @bqnrs = Integer(job(cmd).data.first[:rows_affected])
    rescue StandardError
      @bqnrs = 0
    end
  end
end
